"""
热点问题接口的数据结构
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Any


class QuestionInfo(BaseModel):
    """问题信息"""
    question_id: str = Field(..., description='要添加的问题id')
    question: str = Field(..., description='要添加的问题本身')
    standard_reply: str = Field(..., description='该问题的标准回答')
    related_links: Optional[List[str]] = Field([],description='与该问题有关的相关资料链接')
    category: str = Field("通用", description='添加问题的分类')


class AddQuestionRequest(BaseModel):
    """添加热点问题的数据结构"""
    question_info:QuestionInfo
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')


class AddQuestionBatchRequest(BaseModel):
    question_info_list : list[QuestionInfo]
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')


class UpdateQuestionRequest(BaseModel):
    """更新热点问题的数据结构"""
    question_id: str = Field(..., description='必填项, 要更新的问题id')
    question: Optional[str] = Field(None, description='可选项，要更新的问题本身')
    standard_reply: Optional[str] = Field(None, description='可选项，要更新的标准回复')
    related_links: Optional[List[str]] = Field(None, description='可选项，要更新的相关链接')
    category: Optional[str] = Field(None, description='可选项，要更新的问题分类')


class QueryRequest(BaseModel):
    """询问热点问题"""
    query: str
    limit: Optional[int] = 3
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')



class ApiResponse(BaseModel):
    """回复数据"""
    code: int
    status: str
    message: str
    data: Optional[Any] = None