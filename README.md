# painel_cfpm

 ![](https://img.shields.io/static/v1?label=&message=Nice&color=green)
 ![](https://img.shields.io/badge/crazy-blueviolet?style=for-the-badge)

### Dicas Úteis

1. Importar variáveis de ambiente
   ```
   set -o allexport; source .devcontainer/.env; set +o allexport
   ```
2. Resolver Import "X" could not be resolved from source Pylance
   ```
   1. Open the Command Palette by pressing Ctrl+Shift+P on your keyboard.
   In the Command Palette, select Python: Clear Cache and Reload Window.
   ```