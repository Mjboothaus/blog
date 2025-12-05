default:
  @just --list


# View/edit DuckDB database with DuckDB CLI / WebUI
duck database:
    duckdb --ui {{database}}


# reqs:
#     TODO


render:
    quarto render titanic/titanic-notebook-review.ipynb
    

view:
    open titanic/titanic-notebook-review.html 
