"""
ML module for tool selection policy training.

Stages:
1. bootstrap_data.py  — Generate training data from Jina reranker ($200 budget)
2. model.py           — Unified Policy+Reward model (Qwen 2.5 + dual heads)
3. train_sft.py       — Supervised fine-tuning on Jina scores
4. train_dpo.py       — Direct Preference Optimization on preference pairs
5. self_improve.py    — Online self-improvement from execution feedback
6. serve.py           — Modal inference server
7. integration.py     — Wire into recommend_tools endpoint
"""
