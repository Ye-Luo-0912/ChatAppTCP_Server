#!/usr/bin/env python3
"""在 relgate 上为 PushWorker 注入 WebPush VAPID 密钥配置"""
import json

with open("appsettings.json") as f:
    c = json.load(f)

c["Push"]["Enabled"] = True
c["Push"]["ProviderMode"] = "TestNoop"

# WebPush VAPID（冒烟验证密钥对；生产替换为正式密钥）
if "Providers" not in c:
    c["Providers"] = {}
c["Providers"]["WebPush"] = {
    "VapidSubject": "mailto:push@chatapp.relgate",
    "VapidPublicKey": "BNQ4l_CrhgcTbT2KGWNaoqjwT6gxyPV6bOG3UtHlOKDevsrODOFGY7ekTNIS7PD97SVconCCMIn9LfdQb3Ja3wQ",
    "VapidPrivateKeyPem": "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JR0hBZ0VBTUJNR0J5cUdTTTQ5QWdFR0NDcUdTTTQ5QXdFSEJHMHdhd0lCQVFRZ3J2VTdrS1lzUE1FbDhXRjgKN0hWRFhpQzQ0OHRQYWZ5dCtyanVUYTdWcFVXaFJBTkNBQVRVT0pmd3E0WUhFMjA5aWhsaldxS284RStvTWNqMQplbXpodDFMUjVUaWczcjdLemd6aFJtTzNwRXpTRXV6dy9lMGxYS0p3Z2pDSi9TMzNVRzl5V3Q4RQotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tCg=="
}

with open("appsettings.json", "w") as f:
    json.dump(c, f, indent=2)

print("PushWorker VAPID configured:")
print(f"  VapidPublicKey: {c['Providers']['WebPush']['VapidPublicKey'][:20]}...")
print(f"  Push.Enabled: {c['Push']['Enabled']}")
print(f"  ProviderMode: {c['Push']['ProviderMode']}")
