import requests
import json
import ast


url = "http://127.0.0.1:5001/cex/api/extractor"

# Paths to your PDF files
file_paths = [
    r"C:\Users\pracownik\Downloads\JTokarskaBakir_HannaKrall_ChildhoodAsAnUnfinishedSentence_ForumOfPoetics_2_2025.pdf"
]

# Prepare files for multipart upload
files = []
for path in file_paths:
    files.append(
        ("input_files_or_archives", (path, open(path, "rb"), "application/pdf"))
    )

# Form data (non-file fields)
data = {
    "perform_alignment": "true",
    "create_rdf": "true",
    "max_workers": "24",
    "consolidateCitations": '1'
}

# Send request
response = requests.post(url, files=files, data=data)

# Check response
print(response.status_code)
print(response.text)

type(ast.literal_eval(response.text))


# Send GET request (streaming for large files)
response = requests.post(json.dumps(response.text)["download_url"], stream=True)

# Extract filename from URL (like curl -O does)
filename = url.split("/")[-1]

# Save file
with open(filename, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)

print(f"Downloaded: {filename}")



dir(ast)
