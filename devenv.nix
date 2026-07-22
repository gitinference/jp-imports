{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:
{
  # --- 1. Global Base Configuration ---
  # Variables and settings that apply absolutely everywhere (Dev & Prod)
  env = {
    GREET = "devenv";
  };

  dotenv.enable = true;

  imports = [
    ./devenv # Loads ./devenv/default.nix
  ];

  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  # --- 2. Profile Definitions ---
  profiles = {
    # Development Environment Profile
    dev.module = { config, ... }: {
      env = {
        UV_SYSTEM_PYTHON = "0";
        OCO_AI_PROVIDER = "ollama";
        OCO_PROMPT_MODULE = "conventional-commit";
        OCO_MODEL = "qwen2.5-coder:3b";
      };

      packages = with pkgs; [
        git
        git-cliff
        opencommit
        jupyter
        nixpkgs-fmt
        docker
        skopeo
        podman
        docker-buildx
      ];

      enterShell = ''
        hello
        git --version
        export OCO_API_CUSTOM_HEADERS="{\"Authorization\": \"Bearer $OLLAMA_API_KEY\"}"
      '';

      enterTest = ''
        echo "Running tests"
        git --version | grep --color=auto "${pkgs.git.version}"
      '';
    };

    # Container / Production Environment Profile
    container-build.module = {
      env = {
        UV_SYSTEM_PYTHON = "1";
        NODE_ENV = "production";
        # Add any other production-specific environment variables here
      };

      packages = with pkgs; [
        docker
        docker-buildx
        podman
        skopeo
      ];

      languages.python.uv.sync.enable = false;
      languages.python.lsp.enable = false;
    };
  };
}
