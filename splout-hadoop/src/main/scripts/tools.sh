#!/usr/bin/env bash
#
# Launches Splout tools
#

# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

$bin/launch.sh com.splout.db.ToolsDriver $*
