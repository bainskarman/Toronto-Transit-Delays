@echo off
echo Installing dependencies...
pip install -r requirements.txt

echo Running data update...
python update_data.py

echo Data update completed!
pause