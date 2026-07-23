autobot = AutonomousAgentServiceClient("development")
from typing import List, Dict, Any, Union
import requests

class DynamicSchemaClient:
    def __init__(self, base_url: str = "http://localhost:8010"):
        self.base_url = base_url

    def extract(self, texts: List[str], dynamic_schema: Dict[str, str]) -> Dict[str, Any]:
        url = f"{self.base_url}/extract"
        payload = {
            "texts": texts,
            "dynamic_schema": dynamic_schema
        }
        response = requests.post(url, json=payload)
        return self._handle_response(response)

    def infer(self, texts: Union[List[str], str], temperature: float = 1, n: int = 1, multiplier: float = 1.0, extract: bool = True) -> Dict[str, Any]:
        url = f"{self.base_url}/infer"
        payload = {
            "texts": texts,
            "temperature": temperature,
            "n": n,
            "multiplier": multiplier,
            "extract": extract
        }
        response = requests.post(url, json=payload)
        return self._handle_response(response)

    def health(self) -> Dict[str, Any]:
        url = f"{self.base_url}/health"
        response = requests.get(url)
        return self._handle_response(response)

    def root(self) -> Dict[str, Any]:
        url = f"{self.base_url}/"
        response = requests.get(url)
        return self._handle_response(response)

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 422:
            raise ValueError(f"Validation error: {response.json()}")
        else:
            response.raise_for_status()


autobot.auth(user_data={
    "user_id": "arthur",
    "user_email": "arthur@brainchain.ai"
})

# To stream the completion

prompt = """
You will show your thinking and then answer the question. 
Make sure your answer is enclosed in 

<ANSWER> [ A | B | C | D ] </ANSWER> 

tags and any other thoughts you have. 
You will have additional data provided to you as well. Always answer based on the information provided and your step by step thinking

"""
exam_buffer = []

import fitz
import cv2
import numpy as np
import base64
import os
import requests
import json
import time
from typing import List, Dict, Any, Optional

# Convert PDF to images
def pdf_to_images(pdf_path):
    pdf_document = fitz.open(pdf_path)
    image_paths = []
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        pix = page.get_pixmap()
        output_path = f"page_{page_num + 1}.png"
        pix.save(output_path)
        image_paths.append(output_path)
    return image_paths

# Convert image to base64
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

# Main function
def process_exam_data(pdf_path: str, openai_api_key: str, extracted: Optional[List] = None, objects: List = []) -> List[Dict[str, Any]]:
    if extracted is None:
        extracted = []

    # Convert PDF to images
    image_paths = pdf_to_images(pdf_path)

    # Convert each image to base64 and send to OpenAI API
    for image_path in image_paths[2:]:
        base64_image = encode_image(image_path)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openai_api_key}"
        }
        payload = {
            "model": "gpt-4-vision-preview",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Rewrite everything you see and think step by step. "
                                "Proceed to restate the questions, any data that is presented (like images, etc), "
                                "and remember to clearly state the question numbers, any assumptions, but DO NOT "
                                "select an answer. This is a text transcription and extraction exercise ONLY. "
                                "PLEASE, IF YOU EVER SEE DIAGRAMS or TABLES in IMAGES, make a FAITHFUL ASCII DIAGRAM "
                                "that FAITHFULLY SHOWCASES DATA."
                            )
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{base64_image}" }
                        }
                    ]
                }
            ],
            "max_tokens": 4096
        }

        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()
        print(response_data)
        items = response_data["choices"][0]["message"]["content"]
        print("items: ", items)
        ds = DynamicSchemaClient()

        max_retries = 5
        retry_delay = 1  # Start with 1 second delay

        while True:
            try:
                v = ds.infer(texts=[str(items)], extract=True, n=1, multiplier=0.95, temperature=1.0)
                print(json.dumps(v["items"], indent=2))
                extracted.append(v)
                print(json.dumps(extracted, indent=2))
                break  # If successful, break out of the loop
            except requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}")
                if max_retries > 0:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    max_retries -= 1
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Max retries reached. Moving to next image.")
                    break
            except Exception as e:
                print(f"An error occurred: {e}")
                if max_retries > 0:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    max_retries -= 1
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Max retries reached. Moving to next image.")
                    break

        print(extracted)
    return extracted

def take_exam():
    pdf_path = "/Users/arthur/cfr_exam_2024.pdf"
    answer_key = "https://www.cbp.gov/sites/default/files/2024-05/may_2024_customs_broker_license_exam_answer_key_0.pdf"
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set")
    exam = process_exam_data(pdf_path, openai_api_key)
    return exam

exam_results = take_exam()