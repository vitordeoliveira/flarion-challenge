{
  description = "RegExp Extract DataFusion - Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustc
            cargo
            rustfmt
            rust-analyzer
            clippy
            cargo-machete
            cargo-edit
          ];

          shellHook = ''
            echo "🦀 Rust development environment loaded"
            echo "Available tools:"
            echo "  rustc --version"
            echo "  cargo --version" 
            echo "  rustfmt --version"
            echo "  rust-analyzer --version"
            echo "  cargo clippy --version"
            echo "  cargo machete --version"
            # echo "  cargo edit --version"
          '';
        };
      });
} 
