import requests
from typing import Union, List, Dict, Optional

class DynamicSchemaAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def _make_request(self, method: str, endpoint: str, json: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}{endpoint}"
        response = requests.request(method, url, json=json)
        response.raise_for_status()
        return response.json()

    def extract(self, texts: Union[str, List[str]], dynamic_schema: Dict[str, str], 
                additional_instructions: str = "", model: str = "o1") -> Dict:
        """
        Extract structured data from texts using a dynamic schema.
        """
        payload = {
            "texts": texts,
            "dynamic_schema": dynamic_schema,
            "additional_instructions": additional_instructions,
            "model": model
        }
        return self._make_request("POST", "/extract", json=payload)

    def infer(self, texts: Union[str, List[str]], temperature: float = 1, multiplier: float = 1, n: int = 3,
              extract: bool = True, max_tokens: int = 4096, additional_instructions: str = "") -> Dict:
        """
        Infer a dynamic schema from texts.
        """
        payload = {
            "texts": texts,
            "temperature": temperature,
            "multiplier": multiplier,
            "n": n,
            "extract": extract,
            "max_tokens": max_tokens,
            "additional_instructions": additional_instructions
        }
        return self._make_request("POST", "/infer", json=payload)

    def health_check(self) -> Dict:
        """
        Perform a health check on the API.
        """
        return self._make_request("GET", "/health")

    def root(self) -> Dict:
        """
        Get the root endpoint of the API.
        """
        return self._make_request("GET", "/")

# Example usage:
if __name__ == "__main__":
    api = DynamicSchemaAPI("http://localhost:8000")  # Replace with your actual API URL
    
    # Example health check
    print(api.health_check())
    
    # Example infer request
    infer_result = api.infer(["Sample text 1", "Sample text 2"])
    print(infer_result)
    
    # Example extract request
    extract_result = api.extract(
        texts=["Sample text 1", "Sample text 2"],
        dynamic_schema={"field1": "description1", "field2": "description2"}
    )
    print(extract_result)
