# Contributing

## Branch structure

Please create new features in `feature/<name_of_feature>` branch, when
you are ready please create a pull request to merge your feature into a
`release/<version_of_release>` branch. If there is no applicable release
feel free to create one.

## Development environment

Begin by collecting the repo from [GitHub](https://github.com/ESGF/esgf-core-utils).

To contribute, you should be using `poetry` as your Python package
manager, see <https://python-poetry.org> for installation instructions.
Please ensure that when you add or update dependencies, you use the
`poetry add` or `poetry add --group <group_name>` command to do so. If
you do not, it is likely that the CI will reject your change.

Once you have poetry installed, you should install the library with all
its dependencies:

``` shell
foo@bar:~$ poetry install
```

Then, activate the `pre-commit hooks` run:

``` shell
foo@bar:~$ poetry run pre-commit install
```

When you commit, the following checks will be run:

-   poetry-check (checks the conformity of the pyproject.toml and
    poetry.lock file - **this can modify the lock file in place, which can then be 
    committed**)
-   poetry-lock (ensures an up-to-date lock file - **this can modify the lock file
    in place, which can then be committed**)
-   black (python style formatter - **fixes issues in place, which can then be 
    committed**)
-   isort (python import order checker - **fixes issues in place, which can then be 
    committed**)
-   ruff linter (Python linter)
-   mypy (Python static type analysis)
-   bandit (Python SAST analyis)
-   xenon (McCabe cyclomatic complexity analysis)
-   sphinx (dry-run documentation build)

You can disable the `pre-commit hooks` per commit with the flag
`--no-verify` however all checks will be preformed in the CI.

You can also (and are encouraged to) run the `pre-commit hooks` manually
as often as you like with:

``` shell
foo@bar:~$ git stage -A
foo@bar:~$ poetry run pre-commit run -a
```

## CI Environment

The CI Environment runs the same checks as the `pre-commit hooks` on
push plus the following additional checks:

-   audit (checks all dependencies for vulnerabilities)

Note, that no fixes in place are performed on the CI.
