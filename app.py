import boto3
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import tempfile
import mimetypes

CONFIG_FILE = "config.json"

# ---------- Helpers ----------

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def create_s3_client(endpoint_url, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def sync_minio_to_r2():
    config = load_config()
    r2 = config["r2"]
    minio = config["minio"]
    paths = config["paths"]
    fecha_inicio_str = config.get("fecha_inicio")

    if not fecha_inicio_str:
        print("[ERROR] Debes definir 'fecha_inicio' en el config.json")
        return

    # Zona horaria Lima
    lima_tz = ZoneInfo("America/Lima")

    # Convertir fecha_inicio a Lima
    fecha_inicio = datetime.fromisoformat(fecha_inicio_str).replace(tzinfo=lima_tz)
    now = datetime.now(lima_tz)

    print(f"Sincronizando archivos desde MinIO hacia R2 desde {fecha_inicio} hasta {now}")

    minio_client = create_s3_client(minio["endpoint_url"], minio["access_key"], minio["secret_key"])
    r2_client = create_s3_client(r2["endpoint_url"], r2["access_key"], r2["secret_key"])

    archivos_sincronizados = 0

    for path in paths:
        response = minio_client.list_objects_v2(Bucket=minio["bucket"], Prefix=path)
        if "Contents" not in response:
            print(f"No se encontraron archivos en la ruta: {path}")
            continue

        for obj in response["Contents"]:
            obj_key = obj["Key"]
            # Convertir LastModified de UTC a Lima
            last_modified = obj["LastModified"].astimezone(lima_tz)

            # Comparar con fecha_inicio Lima
            if last_modified < fecha_inicio:
                print(f"[SKIP] {obj_key} es anterior a la fecha_inicio ({last_modified})")
                continue

            print(f"[INFO] Archivo a migrar: {obj_key} - LastModified: {last_modified}")

            # Descargar archivo a temp
            local_file = os.path.join(tempfile.gettempdir(), os.path.basename(obj_key))
            try:
                minio_client.download_file(minio["bucket"], obj_key, local_file)
            except Exception as e:
                print(f"[ERROR] No se pudo descargar {obj_key}: {e}")
                continue

            # Detectar tipo de contenido automáticamente
            content_type, _ = mimetypes.guess_type(local_file)
            if content_type is None:
                content_type = "application/octet-stream"

            # Subir a R2
            try:
                r2_client.upload_file(
                    local_file,
                    r2["bucket"],
                    obj_key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "ContentDisposition": "inline"
                    }
                )
                print(f"[OK] Sincronizado: {obj_key} con ContentType={content_type}")
                archivos_sincronizados += 1
            except Exception as e:
                print(f"[ERROR] No se pudo subir {obj_key} a R2: {e}")
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)

    print(f"Sincronización completada ✅ Total archivos migrados: {archivos_sincronizados}")


if __name__ == "__main__":
    sync_minio_to_r2()
