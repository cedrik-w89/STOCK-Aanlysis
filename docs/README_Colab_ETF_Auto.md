[README_Colab_ETF_Auto.md](https://github.com/user-attachments/files/27964677/README_Colab_ETF_Auto.md)
# Colab ETF 自動化執行版本

## 檔案說明

- `colab_etf_auto_pipeline.py`：合併後主程式，已移除 input() 選單，改成排程友善的 argparse。
- `ETF_Colab_Auto_Run.ipynb`：Colab 執行器，可手動 Run all，也可用 Colab Enterprise 排程。
- `requirements_colab_etf.txt`：Colab 安裝套件清單。

## 執行模式

```bash
python colab_etf_auto_pipeline.py --mode full-today
python colab_etf_auto_pipeline.py --mode full-range --start-date 2026-05-01 --end-date 2026-05-18
python colab_etf_auto_pipeline.py --mode etf-today
python colab_etf_auto_pipeline.py --mode data-range --start-date 2026-05-01 --end-date 2026-05-18
python colab_etf_auto_pipeline.py --mode signal-range --start-date 2026-05-01 --end-date 2026-05-18
```

## 環境變數

請在 Colab Secrets、.env、或執行環境中設定：

```env
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_db
DB_USER=your_user
DB_PASSWORD=your_password
APP_TIMEZONE=Asia/Taipei
```

## 建議正式排程

一般 Colab 適合手動執行或測試；若要正式排程，建議使用 Colab Enterprise Notebook Scheduling，或改用 n8n + Cloud Run Job / VM / GitHub Actions。
