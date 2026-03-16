# Android Traffic Capture Guide

This guide explains how to capture Android app traffic to understand the JWT token flow for PDF downloads.

## Why Traffic Capture?

The eCourts mobile API requires a JWT token for PDF downloads. Our API calls don't receive tokens in responses. We need to capture real app traffic to understand:

1. Which API endpoint provides the JWT token
2. The exact `display_pdf.php` request (URL, headers, cookies)
3. Any special session initialization required

## Setup Options

### Option 1: AnyProxy (Recommended)

**Install:**
```bash
npm install -g anyproxy
```

**Generate CA Certificate:**
```bash
anyproxy-ca
# Creates certificate at ~/.anyproxy/certificates/
```

**Install Certificate on Android:**
1. Transfer `rootCA.crt` to device
2. Settings > Security > Install from storage
3. Select the certificate file
4. Name it "AnyProxy CA"

**Start AnyProxy:**
```bash
anyproxy --port 8001 --web 8002
```

**Configure Android Proxy:**
1. WiFi settings > Modify network
2. Advanced options > Proxy > Manual
3. Proxy hostname: Your computer's IP
4. Proxy port: 8001

### Option 2: mitmproxy

**Install:**
```bash
pip install mitmproxy
```

**Run:**
```bash
mitmweb --listen-port 8080
```

**Certificate Installation:**
- Navigate to `mitm.it` on device
- Download and install Android certificate

### Option 3: Charles Proxy

1. Download Charles Proxy
2. Help > SSL Proxying > Install Charles Root Certificate on Mobile
3. Follow on-screen instructions

## What to Capture

### Step 1: Fresh App Start
1. Force close the eCourts app
2. Clear app data (optional, for clean state)
3. Start traffic capture
4. Open the app

**Look for:**
- `appReleaseWebService.php` response
- Any response containing `"token":` field

### Step 2: Navigate to a Case
1. Select a state (e.g., Telangana)
2. Select a district
3. Select a court complex
4. Search for disposed cases

**Look for:**
- Responses to `stateWebService.php`, `districtWebService.php`, etc.
- Any `"token":` field in responses

### Step 3: Open Case with Judgments
1. Find a disposed case with judgments
2. Open the case history
3. Click on the PDF link

**Look for:**
- Request to `display_pdf.php`
- URL parameters: `params` and `authtoken`
- Request headers (especially `Authorization`)
- Cookies in the request

## Captured Data Template

When you capture the traffic, please note:

```
=== appReleaseWebService.php ===
Response token field: [null / actual value]
Full response: [paste JSON]

=== Any endpoint with token ===
Endpoint: [endpoint name]
Token value: [token string]

=== display_pdf.php request ===
Full URL: [complete URL]
Headers:
  Authorization: [value if present]
  Cookie: [cookie values]
Request params:
  params: [encrypted params]
  authtoken: [encrypted authtoken]
```

## Testing the Debug Script

After capturing traffic, test with:

```bash
cd mobile
python debug_pdf_flow.py --verbose

# Or test a specific captured PDF URL:
python debug_pdf_flow.py --test-url "captured_url_here"
```

## Next Steps

Once you have the captured data:

1. If a token is found in API responses:
   - Update `api_client.py` to call that endpoint first
   - Store and use the token for PDF downloads

2. If the authtoken comes from elsewhere:
   - May need to implement the exact encryption from the app
   - Could be device-specific or time-based

3. Share findings for implementation:
   - Which endpoint returns the token
   - The exact format of the display_pdf.php request
   - Any additional cookies or headers required
