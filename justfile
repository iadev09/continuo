set shell := ["zsh", "-cu"]

default: smoke

check:
    cargo check

check-core:
    cargo check --no-default-features

check-all:
    cargo check --all-features

check-examples:
    cargo check --examples
    cargo check --all-features --examples

smoke: check-core check check-all check-examples

package-list:
    cargo package --allow-dirty --list

tree:
    cargo tree --no-default-features --depth 1
    cargo tree --all-features --depth 1
