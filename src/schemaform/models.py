from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class FormModel(Base):
    __tablename__ = "forms"

    id = Column(String, primary_key=True)
    public_id = Column(String, unique=True, index=True)
    name = Column(String)
    description = Column(Text)
    status = Column(String)
    schema_json = Column(Text)
    field_order = Column(Text)
    webhook_url = Column(Text, nullable=True)
    webhook_on_submit = Column(Integer, default=0)
    webhook_on_delete = Column(Integer, default=0)
    webhook_on_edit = Column(Integer, default=0)
    creator_group_id = Column(Integer, nullable=True, index=True)
    allow_view_others = Column(Integer, default=1)
    allow_edit_submissions = Column(Integer, default=1)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class SubmissionModel(Base):
    __tablename__ = "submissions"

    id = Column(String, primary_key=True)
    form_id = Column(String, index=True)
    data_json = Column(Text)
    user_id = Column(Integer, nullable=True, index=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime, nullable=True)


class SettingModel(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True)
    value = Column(Text)


class FileModel(Base):
    __tablename__ = "files"

    id = Column(String, primary_key=True)
    form_id = Column(String, index=True)
    original_name = Column(String)
    stored_path = Column(Text)
    content_type = Column(String)
    size = Column(Integer)
    created_at = Column(DateTime)
