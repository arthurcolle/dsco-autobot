import requests
import logging
from typing import List, Dict, Any, Optional, Union
import json

# Configure the logger
logger = logging.getLogger("OpenRouterClient")
logging.basicConfig(level=logging.INFO)

class Model:
    """
    a class to enable loading multiple Models
    """
    
class OpenRouterClient:
    """
    A client to interact with the OpenRouter API, handling both chat completions and tool invocations.
    """
    
    def __init__(self, api_key: str, base_url: str = "https://openrouter.ai/api/v1"):
        """
        Initializes the client with the API key and base URL.
        
        Args:
            api_key (str): The API key for authenticating requests.
            base_url (str): The base URL for the OpenRouter API.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def list_models(self) -> List[Dict[str, Any]]:
        """
        Lists all available models from the OpenRouter API.
        
        Returns:
            List[Dict[str, Any]]: A list of model metadata.
        """
        url = f"{self.base_url}/models"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json().get('models', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching models: {e}")
            raise Exception("Failed to list models.") from e
    
    def get_model(self, model_name: str) -> Dict[str, Any]:
        """
        Fetches details of a specific model by its name.
        
        Args:
            model_name (str): The name of the model.
        
        Returns:
            Dict[str, Any]: Details of the requested model.
        """
        url = f"{self.base_url}/models/{model_name}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching model {model_name}: {e}")
            raise Exception(f"Failed to fetch model: {model_name}") from e

    def create_completion(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        stream: bool = False,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[Union[str, Dict[str, Any]]] = None,
        stop: Optional[Union[str, List[str]]] = None,
        n: int = 1
    ) -> Union[Dict[str, Any], requests.Response]:
        """
        Create a chat completion request.
        
        Args:
            model (str): The model to use for completion.
            messages (List[Dict[str, Any]]): List of message objects.
            max_tokens (Optional[int]): The maximum number of tokens to generate.
            temperature (Optional[float]): Sampling temperature.
            top_p (Optional[float]): Nucleus sampling probability.
            stream (bool): Whether to enable streaming.
            tools (Optional[List[Dict[str, Any]]]): List of tools for tool calling.
            tool_choice (Optional[Union[str, Dict[str, Any]]]): Tool choice parameter.
            stop (Optional[Union[str, List[str]]]): Stop sequences.
            n (int): Number of completions to generate.
        
        Returns:
            Dict[str, Any] or requests.Response: The API response or the streaming response object.
        """
        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": model,
            "messages": messages,
            "n": n
        }

        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        if top_p is not None:
            payload["top_p"] = top_p
        if tools is not None:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        if stop is not None:
            payload["stop"] = stop
        if stream:
            payload["stream"] = True

        logger.debug(f"Payload for completion: {json.dumps(payload, indent=2)}")

        try:
            response = requests.post(url, headers=self.headers, json=payload, stream=stream)
            response.raise_for_status()
            if stream:
                return response
            else:
                return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating completion: {e}")
            raise Exception("Failed to create completion.") from e

    def stream_completion(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        **kwargs
    ) -> requests.Response:
        """
        Stream chat completions from the model.
        
        Args:
            model (str): The model to use for streaming completions.
            messages (List[Dict[str, Any]]): List of message objects.
            kwargs: Additional arguments such as max_tokens, temperature, etc.
        
        Returns:
            requests.Response: The streaming response object.
        """
        return self.create_completion(model, messages, stream=True, **kwargs)

    def tool_completion(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a tool-based completion using the model.
        
        Args:
            model (str): The model to use for tool-based completion.
            messages (List[Dict[str, Any]]): List of message objects.
            tools (List[Dict[str, Any]]): List of tools to invoke.
            kwargs: Additional arguments such as max_tokens, temperature, etc.
        
        Returns:
            Dict[str, Any]: The completion response with tool invocations.
        """
        return self.create_completion(model, messages, tools=tools, **kwargs)

