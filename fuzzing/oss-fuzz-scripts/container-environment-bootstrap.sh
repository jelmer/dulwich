#!/usr/bin/env bash

set -euo pipefail

#################
# Prerequisites #
#################

for cmd in python3 git wget zip; do
  command -v "$cmd" >/dev/null 2>&1 || {
    printf '[%s] Required command %s not found, exiting.\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$cmd" >&2
    exit 1
  }
done

#############
# Functions #
#############

download_and_concatenate_common_dictionaries() {
  # Assign the first argument as the target file where all contents will be concatenated
  local target_file="$1"

  # Shift the arguments so the first argument (target_file path) is removed
  # and only URLs are left for the loop below.
  shift

  for url in "$@"; do
    wget -qO- "$url" >>"$target_file"
    # Ensure there's a newline between each file's content
    echo >>"$target_file"
  done
}

prepare_dictionaries_for_fuzz_targets() {
  local dictionaries_dir="$1"
  local fuzz_targets_dir="$2"
  local common_base_dictionary_filename="$WORK/__base.dict"

  printf '[%s] Copying .dict files from %s to %s\n' "$(date '+%Y-%m-%d %H:%M:%S')"  "$dictionaries_dir" "$SRC/"
  cp -v "$dictionaries_dir"/*.dict "$SRC/"

  download_and_concatenate_common_dictionaries "$common_base_dictionary_filename" \
    "https://raw.githubusercontent.com/google/fuzzing/master/dictionaries/utf8.dict" \
    "https://raw.githubusercontent.com/google/fuzzing/master/dictionaries/pem.dict"

  find "$fuzz_targets_dir" -name 'fuzz_*.py' -print0 | while IFS= read -r -d '' fuzz_harness; do
    if [[ -r "$common_base_dictionary_filename" ]]; then
      # Strip the `.py` extension from the filename and replace it with `.dict`.
      fuzz_harness_dictionary_filename="$(basename "$fuzz_harness" .py).dict"
      local output_file="$SRC/$fuzz_harness_dictionary_filename"

      printf '[%s] Appending %s to %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$common_base_dictionary_filename" "$output_file"
      if [[ -s "$output_file" ]]; then
        # If a dictionary file for this fuzzer already exists and is not empty,
        # we append a new line to the end of it before appending any new entries.
        #
        # LibFuzzer will happily ignore multiple empty lines in a dictionary but fail with an error
        # if any single line has incorrect syntax (e.g., if we accidentally add two entries to the same line.)
        # See docs for valid syntax: https://llvm.org/docs/LibFuzzer.html#id32
        echo >>"$output_file"
      fi
      cat "$common_base_dictionary_filename" >>"$output_file"
    fi
  done
}

########################
# Main execution logic #
########################

prepare_dictionaries_for_fuzz_targets "$SRC/dulwich/fuzzing/dictionaries" "$SRC/dulwich/fuzzing/"

apt-get update && apt-get install -y libgpgme-dev libgpg-error-dev

# The OSS-Fuzz base image includes a modified cargo executable for pure rust projects
# but it can cause linking errors with PyO3's extension-module feature so we remove it.
rm -rf /usr/local/bin/cargo
# Install the Rust toolchain so the Rust extensions can be built in build.sh.
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly --profile minimal -y

# The OSS-Fuzz base image has outdated dependencies by default so we upgrade them below.
python3 -m pip install --upgrade pip
# Upgrade to the latest versions known to work at the time the below changes were introduced:
python3 -m pip install -U 'atheris>=2.3.0' 'setuptools~=73.0' 'pyinstaller>=6.10' setuptools-rust
