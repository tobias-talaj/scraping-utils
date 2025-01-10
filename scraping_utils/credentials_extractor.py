import time
import json
import random

import pyautogui

from scraping_utils.mitm_proxy import MitmProxyController


def extract_credentials(keys: list, sequence: function) -> dict: 
    proxy_controller = MitmProxyController(name='monster_co_uk', host='127.0.0.1', port=8080)
    proxy_controller.start()

    try:
        pyautogui.PAUSE = 0.5
        pyautogui.press('win')
        pyautogui.write('terminal')
        pyautogui.press('enter')
        time.sleep(1)
        pyautogui.write('chromium --proxy-server="http://127.0.0.1:8080"')
        pyautogui.press('enter')
        pyautogui.hotkey('ctrl', 'l')
        time.sleep(1)
        pyautogui.write('https://www.monster.co.uk/')
        time.sleep(1)
        pyautogui.press('enter')
        time.sleep(random.uniform(4, 8))

        sequence()

        time.sleep(35)

        credentials = {}
        matching_requests = proxy_controller.find_requests_with_keywords(keys)
        for req in matching_requests:
            headers = req.get('headers')
            payload = req.get('payload')
            if not isinstance(payload, dict):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    print('payload is not valid json')
                    payload = {}
            for k in keys:
                if k in payload:
                    credentials[k] = payload[k]
                elif k in headers:
                    credentials[k] = headers[k]
        return credentials

    finally:
        proxy_controller.stop()
