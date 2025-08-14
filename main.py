import asyncio
import aiohttp
import csv
from datetime import datetime
from urllib.parse import quote
from connection import Connection
import config

# Buscar usuario por barcode
async def get_user_by_barcode(session, token, barcode):
    headers = {
        "X-Okapi-Token": token,
        "X-Okapi-Tenant": config.OKAPI_TENANT,
        "Accept": "application/json"
    }

    query = quote(f'barcode=="{barcode}"')
    url = f"{config.OKAPI_URL}/users?query={query}"

    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            print(f"❌ Error {response.status} al buscar usuario {barcode}")
            return barcode, None
        data = await response.json()
        users = data.get('users', [])
        if not users:
            return barcode, None
        user = users[0]
        user["barcode"] = barcode  # agregar para salida
        return barcode, user

# Procesar barcodes desde archivo TSV
async def process_user_barcodes(token, tsv_path):
    users_found = []
    not_found_barcodes = []

    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    output_csv_path = f"users_salida_{now_str}.csv"
    not_found_log_path = f"no_encontrados_{now_str}.log"

    async with aiohttp.ClientSession() as session:
        with open(tsv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')

            if "barcode" not in reader.fieldnames:
                print("❌ La columna 'barcode' no existe en el archivo.")
                return

            tasks = []
            for row in reader:
                barcode = row.get("barcode")
                if barcode:
                    task = get_user_by_barcode(session, token, barcode)
                    tasks.append(task)

            results = await asyncio.gather(*tasks)

            for barcode, user in results:
                if user:
                    users_found.append(user)
                else:
                    not_found_barcodes.append(barcode)

    # Guardar CSV con usuarios encontrados
    if users_found:
        fieldnames = ["barcode", "username", "active", "firstName", "lastName", "email"]
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            for user in users_found:
                personal = user.get("personal", {})
                row = {
                    "barcode": user.get("barcode", ""),
                    "username": user.get("username", ""),
                    "active": user.get("active", ""),
                    "firstName": personal.get("firstName", ""),
                    "lastName": personal.get("lastName", ""),
                    "email": personal.get("email", "")
                }
                writer.writerow(row)
        print(f"✅ {len(users_found)} usuarios guardados en: {output_csv_path}")
    else:
        print("⚠️ No se encontró ningún usuario.")

    # Guardar barcodes no encontrados
    if not_found_barcodes:
        with open(not_found_log_path, 'w', encoding='utf-8') as f_log:
            for barcode in not_found_barcodes:
                f_log.write(f"{barcode}\n")
        print(f"📄 {len(not_found_barcodes)} barcodes no encontrados guardados en: {not_found_log_path}")
    else:
        print("✅ Todos los barcodes fueron encontrados.")

# Main
def main():
    tsv_path = "barcodes.tsv"
    conn = Connection()
    token = asyncio.run(conn.get_token())
    asyncio.run(process_user_barcodes(token, tsv_path))

if __name__ == "__main__":
    main()
