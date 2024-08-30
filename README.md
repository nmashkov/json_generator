# JSONGenerator
Генератор JSON для Oracle и MSSQL
## Инструкция
- Поместить в папку программы файлы mapping (.xlsx)
- Запустить master.py (JSONGenerator.exe)
- Проверить результаты в файле (название маппинга).json в рабочей папке
## Инструкция по сборке в exe
- активировать пространство venv, проверив расположение pip (which pip)
- установить зависимости из requirements.txt (pip install -r requirements.txt)
- проверить список (pip list), найти auto-py-to-exe
- запустить auto-py-to-exe в cmd
- в окне программы указать путь к файлу master.py
- выбрать One File
- задать название программы в разделе Advanced, --name
- нажать кнопку "Convert .py to .exe"
## Techs
- pandas 2.2.2
## Dev
- Никита Машков, IBS
## Year
- 2024