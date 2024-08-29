# shellcheck shell=bash

set -euo pipefail

unset RUSTFLAGS # The OSS-Fuzz provided RUSTFLAGS cause issues that break PyO3 based Rust extension builds.
export PATH="${PATH}:${HOME}/.cargo/bin"
python3 -m pip install -v ".[fastimport,paramiko,https,pgp]"

find "$SRC" -maxdepth 1 \
  \( -name '*_seed_corpus.zip' -o -name '*.options' -o -name '*.dict' \) \
  -exec printf '[%s] Copying: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" {} \; \
  -exec chmod a-x {} \; \
  -exec cp {} "$OUT" \;

# Build fuzzers in $OUT.
find "$SRC/dulwich/fuzzing" -name 'fuzz_*.py' -print0 | while IFS= read -r -d '' fuzz_harness; do
  compile_python_fuzzer "$fuzz_harness"
done
