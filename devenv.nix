{ pkgs, lib, config, inputs, ... }:
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
