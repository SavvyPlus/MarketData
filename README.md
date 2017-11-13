# MarketData

## Setup

  - Check out source code from Git
  - Create virtualenv for the project
```
        virtualenv <path to project:i.e C:\MarketData>
```
  - Activate virtualenv of the project
```
        <path to project:i.e C:\MarketData>\Scripts\activate
```
  - Instal dependencies
```
        cd <path to project:i.e C:\MarketData>
        pip install -r requirements.txt
```
  - Instal ceODBC
```
        download the ceODBC-2.0.1-cp27-cp27m-win_amd64.whl file from https://www.lfd.uci.edu/~gohlke/pythonlibs/
        pip install ceODBC-2.0.1-cp27-cp27m-win_amd64.whl
```
## SavvyDataLoader

### Build
  - Activate virtualenv of the project
```
        <path to project:i.e C:\MarketData>\Scripts\activate
```
  - Run build script
```
        pyinstaller -F -p C:\MarketData\Lib -p C:\MarketData\Lib\site-packages C:\MarketData\SavvyDataLoader.py    
```
### Deploy
 - Copy `SavvyDataLoader.exe` from  <path to project:i.e C:\MarketData>\dist and `SavvyDataLoader.cfg` and `logging_config.json` from <path to project:i.e C:\MarketData> to deployment folder