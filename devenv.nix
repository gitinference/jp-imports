{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:
{
  # https://devenv.sh/basics/
  env = {
    GREET = "devenv";
    OCO_AI_PROVIDER = "ollama";
    OCO_PROMPT_MODULE = "conventional-commit";
    OCO_MODEL = "qwen2.5-coder:3b";
  };

  dotenv.enable = true;

  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    git-cliff
    opencommit
    jupyter
    nixpkgs-fmt
  ];

  # https://devenv.sh/languages/
  languages.python = {
    enable = true;
    package = pkgs.python312;
    lsp.enable = true;
    venv.enable = true;

    uv = {
      enable = true;
      sync.enable = true;
    };
  };

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  # Project release and version bumping script
  scripts.release.exec = ''
    set -e

    # Ensure we are running from the repository root
    cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

    FORCE=false

    usage() {
      echo "Usage: release [options] VERSION"
      echo
      echo "VERSION:"
      echo "  major: bump major version number"
      echo "  minor: bump minor version number"
      echo "  patch: bump patch version number"
      echo
      echo "Options:"
      echo "  -f, --force:  force release"
      echo "  -h, --help:   show this help message"
      exit 1
    }

    # parse args
    while [ "$#" -gt 0 ]; do
      case "$1" in
      -f | --force)
        FORCE=true
        shift
        ;;
      -h | --help)
        usage
        ;;
      *)
        break
        ;;
      esac
    done

    # check if version is specified
    if [ "$#" -ne 1 ]; then
      usage
    fi

    if [ "$1" != "major" ] && [ "$1" != "minor" ] && [ "$1" != "patch" ]; then
      usage
    fi

    # check if git is clean and force is not enabled
    if ! git diff-index --quiet HEAD -- && [ "$FORCE" = false ]; then
      echo "Error: git is not clean. Please commit all changes first."
      exit 1
    fi

    if ! command -v uv &>/dev/null; then
      echo "Error: uv is not installed. Please install uv from https://docs.astral.sh/uv/"
      exit 1
    fi

    # Check if git-cliff is installed
    if ! command -v git-cliff &>/dev/null; then
      echo "Error: git-cliff is not installed. Please install it first."
      exit 1
    fi

    echo "Would bump version:"
    uv version --bump "$1" --dry-run

    # prompt for confirmation
    if [ "$FORCE" = false ]; then
      read -p "Do you want to release? [yY] " -n 1 -r
      echo
    else
      REPLY="y"
    fi
    echo

    if [[ $REPLY =~ ^[Yy]$ ]]; then
      # 1. Replace version number in pyproject.toml
      uv version --bump "$1"
      new_version=$(uv version --short)

      # 2. Generate/update CHANGELOG.md using git-cliff
      echo "Generating changelog for v$new_version..."
      git-cliff --tag "v$new_version" --output CHANGELOG.md

      # 3. Stage and commit changes (including CHANGELOG.md)
      git add pyproject.toml uv.lock CHANGELOG.md
      git commit -m "bump version to $new_version"
      git tag -a "v$new_version" -m "v$new_version"

      # 4. Push changes
      git push origin main
      git push origin "v$new_version"
    else
      echo "Aborted."
      exit 1
    fi
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    hello
    git --version
    export OCO_API_CUSTOM_HEADERS="{\"Authorization\": \"Bearer $OLLAMA_API_KEY\"}"
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/git-hooks/
  git-hooks.hooks = {
    # 1. The Jupyter Notebook Clear Output Hook
    nixfmt.enable = true;

    prettier = {
      enable = true;
      excludes = [ "CHANGELOG\\.md" ];
    };

    jupyter-nb-clear-output = {
      enable = true;
      name = "jupyter-nb-clear-output";
      entry = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace";
      files = "\\.ipynb$";
      stages = [ "pre-commit" ];
    };

    # 2. Automatically update uv.lock and requirements.txt
    uv-lock-and-requirements = {
      enable = true;
      name = "uv-lock-and-requirements";
      entry = "sh -c 'uv lock && uv export --format requirements.txt -o requirements.txt && git add uv.lock requirements.txt'";
      files = "^pyproject\\.toml$";
      stages = [ "pre-commit" ];
      pass_filenames = false;
    };
  };

  # See full reference at https://devenv.sh/reference/options/
}
