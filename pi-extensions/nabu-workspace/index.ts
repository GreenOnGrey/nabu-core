/**
 * nabu-workspace — Pi's built-in file and shell tools, executed in the
 * workspace of the session (FTR.NAB.CMN-0001 arch §5.1): the user's personal
 * sandbox or a workspace of a calling service (the runner of Hammurapi).
 *
 * The agent (Pi) runs in the agent operator; the files and the commands live
 * in the workspace, which keeps a WebSocket channel to the relay of Nabu. This
 * extension keeps Pi's own read, write, edit, bash, ls, find and grep — their
 * schemas, truncation, diffs and result formats — and replaces only their
 * operations with calls to the relay (NABU_WORKSPACE_URL, NABU_WORKSPACE_TOKEN),
 * which forwards them over the workspace's channel.
 *
 * Loaded only in sessions with a workspace, from the image
 * (/opt/nabu/pi-extensions); it cannot be installed or changed through the
 * administration.
 */

import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import {
	type BashOperations,
	createBashTool,
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createWriteTool,
	DEFAULT_MAX_BYTES,
	type EditOperations,
	type FindOperations,
	formatSize,
	type GrepToolDetails,
	type GrepToolInput,
	type LsOperations,
	type ReadOperations,
	truncateHead,
	type WriteOperations,
} from "@earendil-works/pi-coding-agent";

const URL_BASE = (process.env.NABU_WORKSPACE_URL ?? "").replace(/\/+$/, "");
const TOKEN = process.env.NABU_WORKSPACE_TOKEN ?? "";
const NOTE = process.env.NABU_WORKSPACE_NOTE ?? "";
const IMAGE_TYPES: Record<string, string> = { ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp" };

class WorkspaceError extends Error {
	constructor(
		readonly status: number,
		readonly code: string,
		message: string,
	) {
		super(message);
	}
}

/** Paths are relative to the checkout; the local working directory is only a mount point. */
function remotePath(localCwd: string, p: string): string {
	const rel = path.relative(localCwd, path.resolve(localCwd, p));
	return rel.split(path.sep).join("/");
}

async function call<T>(op: string, body: unknown, signal?: AbortSignal): Promise<T> {
	const res = await fetch(`${URL_BASE}/v1/${op}`, {
		method: "POST",
		headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" },
		body: JSON.stringify(body),
		signal,
	});
	if (res.status === 204) return undefined as T;
	const text = await res.text();
	if (!res.ok) {
		let code = "failed";
		let message = text;
		try {
			const e = JSON.parse(text);
			code = e.error ?? code;
			message = e.message ?? message;
		} catch { }
		if (res.status === 404) {
			const err = new WorkspaceError(404, code, message) as WorkspaceError & { code: string };
			(err as unknown as { code: string }).code = "ENOENT";
			throw err;
		}
		throw new WorkspaceError(res.status, code, `${code}: ${message}`);
	}
	return JSON.parse(text) as T;
}

function readOps(localCwd: string): ReadOperations {
	return {
		readFile: async (p) => Buffer.from((await call<{ data: string }>("fs/read", { path: remotePath(localCwd, p) })).data, "base64"),
		access: async (p) => {
			await call("fs/access", { path: remotePath(localCwd, p) });
		},
		detectImageMimeType: async (p) => IMAGE_TYPES[path.extname(p).toLowerCase()] ?? null,
	};
}

function writeOps(localCwd: string): WriteOperations {
	return {
		writeFile: async (p, content) => {
			await call("fs/write", { path: remotePath(localCwd, p), content: Buffer.from(content).toString("base64") });
		},
		mkdir: async (dir) => {
			await call("fs/mkdir", { path: remotePath(localCwd, dir) });
		},
	};
}

function editOps(localCwd: string): EditOperations {
	const r = readOps(localCwd);
	return {
		readFile: r.readFile,
		writeFile: writeOps(localCwd).writeFile,
		access: async (p) => {
			await call("fs/access", { path: remotePath(localCwd, p), write: true });
		},
	};
}

type Stat = { exists: boolean; isDir?: boolean };

function lsOps(localCwd: string): LsOperations {
	return {
		exists: async (p) => (await call<Stat>("fs/stat", { path: remotePath(localCwd, p) })).exists,
		stat: async (p) => {
			const st = await call<Stat>("fs/stat", { path: remotePath(localCwd, p) });
			if (!st.exists) throw Object.assign(new Error(`ENOENT: ${p}`), { code: "ENOENT" });
			return { isDirectory: () => st.isDir === true };
		},
		readdir: async (p) => (await call<{ entries: string[] }>("fs/readdir", { path: remotePath(localCwd, p) })).entries,
	};
}

function findOps(localCwd: string): FindOperations {
	return {
		exists: async (p) => (await call<Stat>("fs/stat", { path: remotePath(localCwd, p) })).exists,
		glob: async (pattern, cwd, options) =>
			(await call<{ paths: string[] }>("fs/glob", { pattern, path: remotePath(localCwd, cwd), ignore: options.ignore, limit: options.limit })).paths,
	};
}

function bashOps(localCwd: string): BashOperations {
	return {
		exec: async (command, cwd, { onData, signal, timeout }) => {
			const res = await fetch(`${URL_BASE}/v1/exec`, {
				method: "POST",
				headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" },
				body: JSON.stringify({ command, cwd: remotePath(localCwd, cwd), timeoutSec: timeout }),
				signal,
			});
			if (!res.ok || !res.body) throw new Error(`workspace exec: ${res.status} ${await res.text()}`);
			const onAbort = () => {
				void call("abort", {}).catch(() => { });
			};
			signal?.addEventListener("abort", onAbort, { once: true });
			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			let buf = "";
			let final: { exitCode?: number; timedOut?: boolean; aborted?: boolean; timeoutSec?: number } = {};
			try {
				for (; ;) {
					const { value, done } = await reader.read();
					if (done) break;
					buf += decoder.decode(value, { stream: true });
					let i: number;
					// NDJSON records are split on LF only.
					while ((i = buf.indexOf("\n")) >= 0) {
						const line = buf.slice(0, i);
						buf = buf.slice(i + 1);
						if (!line.trim()) continue;
						const rec = JSON.parse(line);
						if (typeof rec.data === "string") onData(Buffer.from(rec.data, "base64"));
						if ("exitCode" in rec) final = rec;
					}
				}
			} finally {
				signal?.removeEventListener("abort", onAbort);
			}
			if (signal?.aborted || final.aborted) throw new Error("aborted");
			if (final.timedOut) throw new Error(`timeout:${final.timeoutSec ?? timeout}`);
			return { exitCode: final.exitCode ?? null };
		},
	};
}

/** grep searches in the workspace; the result is formatted like the built-in tool. */
async function grep(localCwd: string, params: GrepToolInput, signal?: AbortSignal) {
	const limit = Math.max(1, params.limit ?? 100);
	const r = await call<{ lines: string[]; matchCount: number; matchLimitReached: boolean; linesTruncated: boolean }>(
		"grep",
		{
			pattern: params.pattern,
			path: remotePath(localCwd, params.path ?? "."),
			glob: params.glob,
			ignoreCase: params.ignoreCase,
			literal: params.literal,
			context: params.context,
			limit,
		},
		signal,
	);
	if (r.matchCount === 0) return { content: [{ type: "text" as const, text: "No matches found" }], details: undefined };
	const truncation = truncateHead(r.lines.join("\n"), { maxLines: Number.MAX_SAFE_INTEGER });
	const details: GrepToolDetails = {};
	const notices: string[] = [];
	let output = truncation.content;
	if (r.matchLimitReached) {
		details.matchLimitReached = limit;
		notices.push(`${limit} matches limit reached`);
	}
	if (r.linesTruncated) {
		details.linesTruncated = true;
		notices.push("long lines truncated");
	}
	if (truncation.truncated) {
		details.truncation = truncation;
		notices.push(`${formatSize(DEFAULT_MAX_BYTES)} limit reached`);
	}
	if (notices.length > 0) output += `\n\n[${notices.join(". ")}]`;
	return { content: [{ type: "text" as const, text: output }], details: Object.keys(details).length > 0 ? details : undefined };
}

export default function (pi: ExtensionAPI) {
	if (!URL_BASE || !TOKEN) {
		throw new Error("nabu-workspace: NABU_WORKSPACE_URL and NABU_WORKSPACE_TOKEN are required");
	}
	const localCwd = process.cwd();
	const read = createReadTool(localCwd, { operations: readOps(localCwd) });
	const write = createWriteTool(localCwd, { operations: writeOps(localCwd) });
	const edit = createEditTool(localCwd, { operations: editOps(localCwd) });
	const bash = createBashTool(localCwd, { operations: bashOps(localCwd) });
	const ls = createLsTool(localCwd, { operations: lsOps(localCwd) });
	const find = createFindTool(localCwd, { operations: findOps(localCwd) });
	const grepTool = createGrepTool(localCwd);

	pi.registerTool(read);
	pi.registerTool(write);
	pi.registerTool(edit);
	pi.registerTool(bash);
	pi.registerTool(ls);
	pi.registerTool(find);
	pi.registerTool({
		...grepTool,
		async execute(_id, params, signal) {
			return grep(localCwd, params as GrepToolInput, signal);
		},
	});

	// The model must not believe the files are on its machine.
	pi.on("before_agent_start", async (event) => {
		const note =
			NOTE ||
			"The working directory is an isolated workspace. Your read, write, edit, bash, ls, find and grep tools operate there; paths are relative to its root.";
		return { systemPrompt: `${event.systemPrompt}\n\n${note}` };
	});
}
