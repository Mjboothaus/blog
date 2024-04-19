default:
  @just --list


# View/edit DuckDB database with Harlequin CLI
duck database:
    harlequin --theme github-dark {{database}}


reqs:
    pdm export --o requirements.txt --without-hashes --prod

