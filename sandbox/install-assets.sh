#!/bin/sh
# install-assets.sh <assets.lock> <root> — downloads the tools and HTML
# assets of a sandbox profile, checks their sha256 and puts the listed
# members of each archive into <root><destination> (FTR.NAB.CMN-0008 R4).
# Runs in a build stage of its own: curl never reaches the profile image.
set -eu
lock=$1 root=$2
tmp=$(mktemp -d)
grep -v '^#' "$lock" | while IFS="$(printf '\t')" read -r name version sha url members dest; do
  [ -n "$name" ] || continue
  file="$tmp/$name.archive"
  curl -fsSL --retry 3 -o "$file" "$url"
  echo "$sha  $file" | sha256sum -c -
  out="$tmp/$name"
  mkdir -p "$out" "$root$dest"
  case $url in
    *.tar.xz) tar -xJf "$file" -C "$out" ;;
    *) tar -xzf "$file" -C "$out" ;;
  esac
  echo "$members" | tr ',' '\n' | while read -r m; do
    cp -R "$out/$m" "$root$dest/"
  done
  printf '%s\t%s\n' "$name" "$version" >>"$root/opt/nabu/assets.versions"
done
rm -rf "$tmp"
