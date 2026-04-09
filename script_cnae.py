name: Atualizar CNAE

permissions:
  contents: write

on:
  schedule:
    - cron: '0 10 * * *'
  workflow_dispatch:

jobs:
  executar:
    runs-on: ubuntu-latest

    steps:
      - name: Baixar código
        uses: actions/checkout@v4

      - name: 🔄 Sincronizar com repositório (ANTES DE TUDO)
        run: |
          git fetch origin main
          git checkout main
          git reset --hard origin/main

      - name: Instalar Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Instalar dependências
        run: |
          pip install pandas requests openpyxl

      - name: Executar script
        run: python script_cnae.py

      - name: Salvar alterações no repositório
        run: |
          git config --global user.name "bot"
          git config --global user.email "bot@github.com"

          git add .

          if git diff --cached --quiet; then
            echo "Sem alterações para commit"
            exit 0
          fi

          git commit -m "Atualização automática CNAE"
          git push origin main
