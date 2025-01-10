import time
import json
from typing import Callable

import pyautogui

from scraping_utils.mitm_proxy import MitmProxyController


def cast_to_dict(input):
    if isinstance(input, list):
        input = input[0]
    if isinstance(input, dict):
        return input
    try:
        input = json.loads(input)
        return input
    except Exception:
        return {}


def extract_credentials(browser: str, keys: list, sequence: Callable) -> dict: 
    proxy_controller = MitmProxyController(name='monster_co_uk', host='127.0.0.1', port=8080)
    proxy_controller.start()

    try:
        pyautogui.PAUSE = 0.5
        pyautogui.press('win')
        pyautogui.write('terminal')
        pyautogui.press('enter')
        time.sleep(1)
        pyautogui.write(f'{browser} --proxy-server="http://127.0.0.1:8080"')
        pyautogui.press('enter')
        pyautogui.hotkey('ctrl', 'l')

        sequence()

        time.sleep(5)
        pyautogui.hotkey('alt', 'f4')

        credentials = {}
        matching_requests = proxy_controller.find_requests_with_keywords(keys)
        for req in matching_requests:
            headers = cast_to_dict(req.get('headers'))
            payload = cast_to_dict(req.get('payload'))
            headers = {k.lower(): v for k, v in headers.items()}
            payload = {k.lower(): v for k, v in payload.items()}
            keys = [k.lower() for k in keys]

            for k in keys:
                if k in payload:
                    credentials[k] = payload[k]
                elif k in headers:
                    credentials[k] = headers[k]
        return credentials

    finally:
        proxy_controller.stop()
        pyautogui.hotkey('alt', 'f4')
