# devenv/scripts.nix
{ ... }:

{
  scripts = {
    # ../ reaches out of the devenv/ folder to find the root scripts/ folder
    release.exec = builtins.readFile ../scripts/release.sh;
    prod-deploy.exec = builtins.readFile ../scripts/prod-build.sh;
  };
}
