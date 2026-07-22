{ pkgs, ... }:

{
  # https://devenv.sh/git-hooks/
  git-hooks.hooks = {
    nixfmt.enable = true;
    prettier = {
      enable = true;
      excludes = [ "CHANGELOG\\.md" ];
    };
    jupyter-nb-clear-output = {
      enable = true;
      name = "jupyter-nb-clear-output";
      package = pkgs.jupyter;
      entry = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace";
      files = "\\.ipynb$";
      stages = [ "pre-commit" ];
    };
    uv-lock-and-requirements = {
      enable = true;
      name = "uv-lock-and-requirements";
      package = pkgs.uv;
      entry = "sh -c 'uv lock && uv export --format requirements.txt --all-extras --no-emit-workspace -o requirements.txt && git add uv.lock requirements.txt'";
      files = "^pyproject\\.toml$";
      stages = [ "pre-commit" ];
      pass_filenames = false;
    };
  };
}
