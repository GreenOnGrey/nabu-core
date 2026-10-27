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
 * It also gives the agent the tool subagent (FTR.NAB.CMN-0009): 1–4 child
 * agents started by the agent operator of the pod over loopback
 * (NABU_SUBAGENT_URL, NABU_SUBAGENT_TOKEN); a subagent itself has none
 * (NABU_SUBAGENT_DEPTH=1). The call waits for the children and ends with the
 * tool: nothing runs in the background.
 *
 * Loaded only in sessions with a workspace or with subagents, from the image
 * (/opt/nabu/pi-extensions); it cannot be installed or changed through the
 * administration.
 */

import fs from "node:fs/promises";
import os from "node:os";
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
	defineTool,
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
import { Type } from "typebox";

const URL_BASE = (process.env.NABU_WORKSPACE_URL ?? "").replace(/\/+$/, "");
const TOKEN = process.env.NABU_WORKSPACE_TOKEN ?? "";
const NOTE = process.env.NABU_WORKSPACE_NOTE ?? "";
// The working directory as the commands of the workspace see it (/work); "."
// when the engine does not know it.
const ROOT = process.env.NABU_WORKSPACE_ROOT || ".";
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

/**
 * Paths are relative to the checkout; the local working directory is only a mount point. An
 * absolute path elsewhere is the workspace's own (the shell there shows /work/report.md): the
 * workspace server resolves it against its root.
 */
function remotePath(localCwd: string, p: string): string {
	const abs = path.resolve(localCwd, p);
	const rel = path.relative(localCwd, abs);
	if (path.isAbsolute(p) && (rel === ".." || rel.startsWith(`..${path.sep}`))) return abs.split(path.sep).join("/");
	return rel.split(path.sep).join("/");
}

/** Folder of the workspace for the files Pi writes for the model (FTR.NAB.CMN-0001 tech, decision 18). */
const OUTPUT_DIR = ".pi-output";
/** Files of OUTPUT_DIR older than this are removed: they served sessions that are over. */
const OUTPUT_KEEP_MINUTES = 24 * 60;
/** What the model is told of OUTPUT_DIR: its own files there would be removed (tech, decision 24). */
const OUTPUT_RULE = `${OUTPUT_DIR} holds the full output of your tools and is cleaned after a day: never keep your own files there (scripts, drafts, previews), put them in the working directory.`;

/** outputPath tells whether a path of the workspace is inside OUTPUT_DIR. */
function outputPath(remote: string): boolean {
	let p = remote;
	if (ROOT !== "." && (p === ROOT || p.startsWith(`${ROOT}/`))) p = p.slice(ROOT.length).replace(/^\/+/, "");
	return p === OUTPUT_DIR || p.startsWith(`${OUTPUT_DIR}/`);
}

/** The write and edit tools refuse OUTPUT_DIR: only Pi writes there. */
function ownPath(localCwd: string, p: string): string {
	const remote = remotePath(localCwd, p);
	if (outputPath(remote)) throw new WorkspaceError(403, "output_dir_reserved", `output_dir_reserved: ${OUTPUT_RULE}`);
	return remote;
}

/**
 * Pi saves the full text of a truncated result (bash, MCP) to its own temporary directory and
 * names it in details.fullOutputPath and in the text; the model reads it with read or bash, which
 * run in the workspace, where the file is not. The file is moved to OUTPUT_DIR of the workspace.
 * Returns the new path, or undefined when the file stays where Pi wrote it.
 */
async function moveOutputFile(local: string): Promise<string | undefined> {
	if (!local.startsWith(path.join(os.tmpdir(), "pi-"))) return undefined;
	const remote = `${OUTPUT_DIR}/${path.basename(local)}`;
	try {
		const data = await fs.readFile(local);
		await call("fs/write", { path: remote, content: data.toString("base64") });
		await fs.rm(local, { force: true });
		return remote;
	} catch {
		return undefined; // the model keeps the truncated text; the note stays as Pi wrote it
	}
}

/**
 * «Tool mcp__docgen__convert not found»: the model called a deferred tool before tool_search loaded
 * it (FTR.NAB.CMN-0001 tech, decision 20). The mark in details tells the agent operator the call was
 * a load, not a failure.
 */
const NOT_LOADED = /^Tool (\S+) not found$/;
const LOADED_MARK = { nabuToolLoaded: true };

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
			await call("fs/write", { path: ownPath(localCwd, p), content: Buffer.from(content).toString("base64") });
		},
		mkdir: async (dir) => {
			await call("fs/mkdir", { path: ownPath(localCwd, dir) });
		},
	};
}

function editOps(localCwd: string): EditOperations {
	const r = readOps(localCwd);
	return {
		readFile: r.readFile,
		writeFile: writeOps(localCwd).writeFile,
		access: async (p) => {
			await call("fs/access", { path: ownPath(localCwd, p), write: true });
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

const SUBAGENT_URL = process.env.NABU_SUBAGENT_URL ?? "";
const SUBAGENT_TOKEN = process.env.NABU_SUBAGENT_TOKEN ?? "";
const SUBAGENT_MAX = Number(process.env.NABU_SUBAGENT_MAX_PARALLEL) || 4;

// FTR.NAB.CMN-0009 tech §2.4: the words skills use for it are in the description, so the model finds it
const SUBAGENT_DESCRIPTION =
	`Run 1–${SUBAGENT_MAX} subagents in parallel. Each subagent is a fresh agent with your tools, workspace and skills but without this conversation: ` +
	"give it everything it needs in the prompt. You get back only each subagent's final message. Use it when a skill asks to spawn a sub-agent / use " +
	"the Agent tool, to keep long reading out of your context, or to run independent parts in parallel. Subagents cannot start subagents.";

interface SubagentResult { task: number; state: string; text: string; truncated: boolean; error?: string }
type SubagentLine = { task: number; state: string; tools: number } | { results: SubagentResult[] } | { error: string; message: string };

const SUBAGENT_STATE: Record<string, string> = { running: "running", done: "done", failed: "failed", timeout: "timed out", aborted: "stopped" };

function subagentTool() {
	return defineTool({
		name: "subagent",
		label: "Subagents",
		description: SUBAGENT_DESCRIPTION,
		parameters: Type.Object({
			tasks: Type.Array(
				Type.Object({
					description: Type.String({ minLength: 1, maxLength: 80, description: "A few words shown to the user while it runs" }),
					prompt: Type.String({ minLength: 1, maxLength: 64000, description: "The whole task: the subagent sees nothing else" }),
					skill: Type.Optional(Type.String({ description: "A skill of yours the subagent starts with" })),
				}),
				{ minItems: 1, maxItems: SUBAGENT_MAX },
			),
		}),
		async execute(toolCallId, params, signal, onUpdate) {
			const res = await fetch(SUBAGENT_URL, {
				method: "POST",
				headers: { Authorization: `Bearer ${SUBAGENT_TOKEN}`, "Content-Type": "application/json" },
				body: JSON.stringify({ call: toolCallId, tasks: params.tasks }),
				signal,
			});
			if (!res.ok || !res.body) {
				const e = (await res.json().catch(() => ({}))) as { error?: string; message?: string };
				return { content: [{ type: "text", text: `${e.error ?? res.status}: ${e.message ?? res.statusText}` }], isError: true, details: undefined };
			}
			const progress = new Map<number, string>();
			let results: SubagentResult[] = [];
			let rest = "";
			const decoder = new TextDecoder();
			const take = (raw: string) => {
				if (!raw.trim()) return;
				const line = JSON.parse(raw) as SubagentLine;
				if ("results" in line) {
					results = line.results;
				} else if ("task" in line) {
					const d = params.tasks[line.task - 1]?.description ?? `task ${line.task}`;
					progress.set(line.task, `${d}: ${SUBAGENT_STATE[line.state] ?? line.state}, tools ${line.tools}`);
					onUpdate?.({ content: [{ type: "text", text: [...progress.values()].join("\n") }], details: undefined });
				}
			};
			for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
				rest += decoder.decode(chunk, { stream: true });
				let i: number;
				while ((i = rest.indexOf("\n")) >= 0) {
					take(rest.slice(0, i));
					rest = rest.slice(i + 1);
				}
			}
			take(rest);
			const text = results
				.map((r) => {
					const d = params.tasks[r.task - 1]?.description ?? `task ${r.task}`;
					const body = [r.text, r.error ? `Error: ${r.error}` : ""].filter(Boolean).join("\n");
					return `## ${d} — ${SUBAGENT_STATE[r.state] ?? r.state}\n${body}`;
				})
				.join("\n\n");
			return { content: [{ type: "text", text: text || "The subagents returned nothing." }], details: undefined };
		},
	});
}

export default function (pi: ExtensionAPI) {
	if (SUBAGENT_URL && SUBAGENT_TOKEN && process.env.NABU_SUBAGENT_DEPTH === "0") {
		pi.registerTool(subagentTool());
	}
	if (!URL_BASE || !TOKEN) return; // a session with subagents and without a workspace
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

	let pruned = false;
	pi.on("tool_result", async (event) => {
		const details = (event as { details?: unknown }).details;
		const local = (details as { fullOutputPath?: unknown } | undefined)?.fullOutputPath;
		if (typeof local !== "string") return undefined;
		const remote = await moveOutputFile(local);
		if (!remote) return undefined;
		if (!pruned) {
			pruned = true;
			void bashOps(localCwd)
				.exec(`find ${OUTPUT_DIR} -type f -mmin +${OUTPUT_KEEP_MINUTES} -delete 2>/dev/null; true`, localCwd, { onData: () => { }, timeout: 30 })
				.catch(() => { });
		}
		const content = event.content.map((block) => (block.type === "text" ? { ...block, text: block.text.split(local).join(remote) } : block));
		return { content, structuredContent: event.structuredContent, details: { ...(details as object), fullOutputPath: remote } };
	});

	// A deferred tool called before tool_search loaded it is loaded at once, one time per tool: the
	// model is told to repeat the call instead of searching for a tool it already knows the name of.
	const autoLoaded = new Set<string>();
	pi.on("message_end", async (event) => {
		const msg = event.message as { role?: string; isError?: boolean; content?: { type: string; text?: string }[] };
		if (msg.role !== "toolResult" || !msg.isError || msg.content?.length !== 1) return undefined;
		const name = NOT_LOADED.exec(msg.content[0].text ?? "")?.[1];
		const tool = name ? pi.getAllTools().find((t) => t.name === name) : undefined;
		if (!name || tool?.exposure !== "deferred" || autoLoaded.has(name)) return undefined;
		autoLoaded.add(name);
		const active = pi.getActiveTools();
		if (!active.includes(name)) pi.setActiveTools([...active, name]);
		return {
			message: {
				...event.message,
				content: [{ type: "text", text: `The tool ${name} is loaded now. Call it again with the same arguments.` }],
				details: LOADED_MARK,
			} as typeof event.message,
		};
	});

	// The model must not believe the files are on its machine.
	pi.on("before_agent_start", async (event) => {
		const note =
			NOTE ||
			"The working directory is an isolated workspace. Your read, write, edit, bash, ls, find and grep tools operate there; paths are relative to its root.";
		// Pi names its own local directory as the working one: the model must see the workspace's.
		const prompt = event.systemPrompt.split(localCwd).join(ROOT);
		return { systemPrompt: `${prompt}\n\n${note} ${OUTPUT_RULE}` };
	});
}
