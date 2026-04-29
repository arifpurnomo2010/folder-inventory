import os
import shutil
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path

from flask import (
    Flask,
    flash,
    redirect,
    render_template_string,
    request,
    send_file,
    url_for,
)

import create_inventory_reta as inventory


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "folder-inventory-dev")
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_UPLOAD_MB", "500")) * 1024 * 1024


PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Folder Inventory Generator</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 820px; margin: 40px auto; padding: 0 20px; line-height: 1.5; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 24px; box-shadow: 0 2px 10px rgba(0,0,0,.05); }
    label { display: block; margin: 14px 0 6px; font-weight: 700; }
    input[type=file], input[type=number] { width: 100%; padding: 10px; box-sizing: border-box; }
    button { margin-top: 18px; padding: 12px 18px; border: 0; border-radius: 8px; cursor: pointer; background: #2563eb; color: white; font-weight: 700; }
    .hint { color: #555; font-size: 14px; }
    .flash { background: #fee2e2; border: 1px solid #fecaca; color: #7f1d1d; padding: 12px; border-radius: 8px; margin-bottom: 16px; }
    pre { background: #f6f8fa; padding: 12px; overflow: auto; border-radius: 8px; }
  </style>
</head>
<body>
  <h1>Folder Inventory Generator</h1>
  <div class="card">
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for message in messages %}
          <div class="flash">{{ message }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    <p>Upload ZIP folder project. Aplikasi akan menjalankan <code>create_inventory_reta.py</code> di server Railway dan mengembalikan file Excel inventory.</p>

    <form action="{{ url_for('run_inventory_web') }}" method="post" enctype="multipart/form-data">
      <label for="archive">Folder ZIP</label>
      <input id="archive" name="archive" type="file" accept=".zip" required>
      <div class="hint">ZIP harus berisi dokumen yang ingin di-scan. Batas upload default: {{ max_upload_mb }} MB.</div>

      <label for="max_concurrent">Max Concurrent Gemini Requests</label>
      <input id="max_concurrent" name="max_concurrent" type="number" min="1" max="30" value="8">
      <div class="hint">Gunakan angka lebih kecil jika sering kena rate limit API.</div>

      <button type="submit">Generate Inventory Excel</button>
    </form>
  </div>

  <h2>Railway Variables</h2>
  <p>Pastikan variable berikut sudah diset di Railway:</p>
  <pre>GEMINI_API_KEY=...
GEMINI_MODEL=gemini-2.0-flash-lite</pre>
</body>
</html>
"""


def safe_extract_zip(zip_path: Path, destination: Path):
    destination = destination.resolve()
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            target = (destination / member.filename).resolve()
            if destination not in target.parents and target != destination:
                raise ValueError(f"Unsafe path in ZIP: {member.filename}")
        archive.extractall(destination)


def find_scan_root(extract_dir: Path) -> Path:
    entries = [entry for entry in extract_dir.iterdir() if not entry.name.startswith("__MACOSX")]
    directories = [entry for entry in entries if entry.is_dir()]
    files = [entry for entry in entries if entry.is_file()]
    if len(directories) == 1 and not files:
        return directories[0]
    return extract_dir


@app.get("/")
def index():
    max_upload_mb = int(app.config["MAX_CONTENT_LENGTH"] / 1024 / 1024)
    return render_template_string(PAGE, max_upload_mb=max_upload_mb)


@app.get("/health")
def health():
    return {"status": "ok", "service": "folder-inventory"}


@app.post("/run")
def run_inventory_web():
    uploaded = request.files.get("archive")
    if not uploaded or not uploaded.filename:
        flash("Pilih file ZIP terlebih dahulu.")
        return redirect(url_for("index"))

    if not uploaded.filename.lower().endswith(".zip"):
        flash("File harus berformat .zip.")
        return redirect(url_for("index"))

    try:
        max_concurrent = int(request.form.get("max_concurrent", "8"))
        if max_concurrent < 1 or max_concurrent > 30:
            raise ValueError
    except ValueError:
        flash("Max concurrent harus angka 1 sampai 30.")
        return redirect(url_for("index"))

    work_dir = Path(tempfile.mkdtemp(prefix="folder_inventory_"))
    try:
        zip_path = work_dir / "input.zip"
        extract_dir = work_dir / "input"
        extract_dir.mkdir()
        output_file = work_dir / "Project Inventory.xlsx"

        uploaded.save(zip_path)
        safe_extract_zip(zip_path, extract_dir)
        scan_root = find_scan_root(extract_dir)

        logs = []
        success = inventory.run_inventory(
            str(scan_root),
            str(output_file),
            max_concurrent=max_concurrent,
            logger=logs.append,
        )
        if not success or not output_file.exists():
            flash("Inventory gagal dibuat. Log terakhir: " + " | ".join(logs[-5:]))
            shutil.rmtree(work_dir, ignore_errors=True)
            return redirect(url_for("index"))

        output_bytes = BytesIO(output_file.read_bytes())
        shutil.rmtree(work_dir, ignore_errors=True)
        return send_file(
            output_bytes,
            as_attachment=True,
            download_name="Project Inventory.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        flash(f"Inventory gagal: {exc}")
        shutil.rmtree(work_dir, ignore_errors=True)
        return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
