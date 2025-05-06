"""
curl 'https://rangareddy.dcourts.gov.in/wp-admin/admin-ajax.php' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'pll_language=en; PHPSESSID=90lpvdopvon66m550q97dqqr7v' \
  -H 'DNT: 1' \
  -H 'Origin: https://rangareddy.dcourts.gov.in' \
  -H 'Referer: https://rangareddy.dcourts.gov.in/case-status-search-by-case-type/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw 'service_type=courtComplex&est_code=TSRA00%2CTSRA03%2CTSRA02%2CTSRA01%2CTSFC07%2CTSRA41&case_type=2&reg_year=2024&case_status=P&scid=82v9pt9zw8z8oj5bzvb8qd3rp9ca7p0h0cwuii2k&tok_41d0289fa740abf1cc02e37d4ed3da39cd5d5bd6=e3e88e0cf085ca23e0fb62d8649fc2f5d6cc65f3&siwp_captcha_value=SfaVK&es_ajax_request=1&submit=Search&action=get_cases_by_year'

}
curl 'https://rangareddy.dcourts.gov.in/wp-admin/admin-ajax.php' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'pll_language=en; PHPSESSID=90lpvdopvon66m550q97dqqr7v' \
  -H 'DNT: 1' \
  -H 'Origin: https://rangareddy.dcourts.gov.in' \
  -H 'Referer: https://rangareddy.dcourts.gov.in/case-status-search-by-case-type/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw 'cino=TSRA000044972024&action=get_cnr_details&es_ajax_request=1'

  Response:
  
"""

import requests
import json

"""
curl 'https://rangareddy.dcourts.gov.in/?_siwp_captcha&id=fbfe4hxp2tcogrgx58zc0cjcsff138na2xxj6gz3' \
  -H 'Accept: image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Connection: keep-alive' \
  -b 'pll_language=en; PHPSESSID=90lpvdopvon66m550q97dqqr7v' \
  -H 'DNT: 1' \
  -H 'Referer: https://rangareddy.dcourts.gov.in/case-status-search-by-case-type/' \
  -H 'Sec-Fetch-Dest: image' \
  -H 'Sec-Fetch-Mode: no-cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36' \
  -H 'sec-ch-ua: "Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"'
"""
import easyocr

reader = easyocr.Reader(["en"])


def get_captcha_value(scid, retry_count=0):
    url = f"https://rangareddy.dcourts.gov.in/?_siwp_captcha&id={scid}"

    response = requests.get(url)
    cookies = response.cookies
    session_id = cookies.get("PHPSESSID")
    image = response.content
    with open("captcha.png", "wb") as f:
        f.write(image)
    result = reader.readtext("captcha.png")
    if retry_count > 10:
        raise Exception("Failed to get captcha value")
    if len(result) == 0:
        return get_captcha_value(scid, retry_count + 1)
    result_text = result[0][1].strip()
    if len(result_text) != 5:
        return get_captcha_value(scid, retry_count + 1)
    return session_id, result_text


def get_cases():
    url = "https://rangareddy.dcourts.gov.in/wp-admin/admin-ajax.php"
    """
    --data-raw 'service_type=courtComplex&est_code=TSRA00%2CTSRA03%2CTSRA02%2CTSRA01%2CTSFC07%2CTSRA41&case_type=2&reg_year=2024&case_status=P&scid=82v9pt9zw8z8oj5bzvb8qd3rp9ca7p0h0cwuii2k&tok_41d0289fa740abf1cc02e37d4ed3da39cd5d5bd6=e3e88e0cf085ca23e0fb62d8649fc2f5d6cc65f3&siwp_captcha_value=SfaVK&es_ajax_request=1&submit=Search&action=get_cases_by_year'

    """
    scid = "697f14db3d0fab2e3394d6df36c31792fbc4a6cd"
    session_id, captch_value = get_captcha_value(scid)
    cookies = {"PHPSESSID": session_id, "pll_language": "en"}
    payload = {
        "service_type": "courtComplex",
        "est_code": "TSRA00,TSRA03,TSRA02,TSRA01,TSFC07,TSRA41",
        "case_type": "2",
        "reg_year": "2024",
        "case_status": "P",
        "scid": scid,
        "tok_41d0289fa740abf1cc02e37d4ed3da39cd5d5bd6": "e3e88e0cf085ca23e0fb62d8649fc2f5d6cc65f3",
        "siwp_captcha_value": captch_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "action": "get_cases_by_year",
    }
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://rangareddy.dcourts.gov.in",
        "Referer": "https://rangareddy.dcourts.gov.in/case-status-search-by-case-type/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    response = requests.post(url, data=payload, cookies=cookies, headers=headers)
    response_text = response.text
    return response_text


get_cases()
