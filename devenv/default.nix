# devenv/default.nix
{ ... }:

{
  imports = [
    ./scripts.nix
    ./pre-commit.nix
    ./languages.nix
  ];
}
