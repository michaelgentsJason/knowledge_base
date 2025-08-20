"""
热点问题路由下的对外API接口定义
"""
from fastapi import APIRouter, Body, BackgroundTasks

from src.utils.logger import logger


router = APIRouter(prefix="/hotspot")

