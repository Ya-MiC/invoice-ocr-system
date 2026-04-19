#!/usr/bin/env python3
"""
OCR Engine - Production-grade OCR processing with multiple backends.
Supports PaddleOCR, Tesseract, and EasyOCR engines.
"""

import asyncio
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np

logger = logging.getLogger(__name__)


class BaseOCREngine(ABC):
    """Abstract base class for OCR engines."""
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the OCR engine."""
        pass
    
    @abstractmethod
    def recognize(self, image: Union[bytes, np.ndarray, str], **kwargs) -> Dict[str, Any]:
        """Recognize text from image."""
        pass
    
    @abstractmethod
    def is_ready(self) -> bool:
        """Check if engine is ready for processing."""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Get engine name."""
        pass


class PaddleOCREngine(BaseOCREngine):
    """PaddleOCR engine implementation - optimized for Chinese invoices."""
    
    def __init__(self, language: str = "ch", use_gpu: bool = False, **kwargs):
        self.language = language
        self.use_gpu = use_gpu
        self.ocr = None
        self._ready = False
        self._config = kwargs
        
    def initialize(self) -> bool:
        """Initialize PaddleOCR engine."""
        try:
            from paddleocr import PaddleOCR
            
            self.ocr = PaddleOCR(
                use_angle_cls=True,
                lang=self.language,
                use_gpu=self.use_gpu,
                show_log=False,
                **self._config
            )
            self._ready = True
            logger.info(f"PaddleOCR initialized: lang={self.language}, gpu={self.use_gpu}")
            return True
        except ImportError:
            logger.error("PaddleOCR not installed. Install with: pip install paddleocr")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize PaddleOCR: {e}")
            return False
    
    def recognize(self, image: Union[bytes, np.ndarray, str], **kwargs) -> Dict[str, Any]:
        """Recognize text using PaddleOCR."""
        if not self.is_ready():
            return {"text": "", "boxes": [], "confidence": 0.0, "error": "Engine not ready"}
        
        try:
            # Handle different input types
            if isinstance(image, bytes):
                # Save bytes to temp file
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    tmp.write(image)
                    tmp_path = tmp.name
                result = self.ocr.ocr(tmp_path, cls=True)
                os.unlink(tmp_path)
            elif isinstance(image, str):
                result = self.ocr.ocr(image, cls=True)
            else:
                # numpy array
                result = self.ocr.ocr(image, cls=True)
            
            # Parse results
            if not result or result[0] is None:
                return {"text": "", "boxes": [], "confidence": 0.0}
            
            texts = []
            boxes = []
            confidences = []
            
            for line in result[0]:
                box, (text, confidence) = line
                texts.append(text)
                boxes.append(box)
                confidences.append(confidence)
            
            full_text = "\n".join(texts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            return {
                "text": full_text,
                "boxes": boxes,
                "confidences": confidences,
                "confidence": avg_confidence,
                "raw_result": result
            }
            
        except Exception as e:
            logger.error(f"PaddleOCR recognition error: {e}")
            return {"text": "", "boxes": [], "confidence": 0.0, "error": str(e)}
    
    def is_ready(self) -> bool:
        return self._ready and self.ocr is not None
    
    def get_name(self) -> str:
        return "PaddleOCR"


class TesseractEngine(BaseOCREngine):
    """Tesseract OCR engine implementation."""
    
    def __init__(self, language: str = "chi_sim+eng", **kwargs):
        self.language = language
        self._ready = False
        self._config = kwargs
        
    def initialize(self) -> bool:
        """Initialize Tesseract engine."""
        try:
            import pytesseract
            from PIL import Image
            
            # Verify Tesseract is available
            pytesseract.get_tesseract_version()
            self._ready = True
            logger.info(f"Tesseract initialized: lang={self.language}")
            return True
        except ImportError:
            logger.error("pytesseract not installed. Install with: pip install pytesseract")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize Tesseract: {e}")
            return False
    
    def recognize(self, image: Union[bytes, np.ndarray, str], **kwargs) -> Dict[str, Any]:
        """Recognize text using Tesseract."""
        if not self.is_ready():
            return {"text": "", "boxes": [], "confidence": 0.0, "error": "Engine not ready"}
        
        try:
            import pytesseract
            from PIL import Image
            import io
            
            # Handle different input types
            if isinstance(image, bytes):
                pil_image = Image.open(io.BytesIO(image))
            elif isinstance(image, str):
                pil_image = Image.open(image)
            else:
                pil_image = Image.fromarray(image)
            
            # Get text with confidence
            data = pytesseract.image_to_data(
                pil_image, 
                lang=self.language,
                output_type=pytesseract.Output.DICT
            )
            
            texts = []
            confidences = []
            
            for i, text in enumerate(data.get("text", [])):
                if text.strip():
                    texts.append(text)
                    conf = data.get("conf", [])[i]
                    confidences.append(float(conf) if conf != -1 else 0.0)
            
            full_text = " ".join(texts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            return {
                "text": full_text,
                "boxes": [],
                "confidences": confidences,
                "confidence": avg_confidence / 100.0  # Normalize to 0-1
            }
            
        except Exception as e:
            logger.error(f"Tesseract recognition error: {e}")
            return {"text": "", "boxes": [], "confidence": 0.0, "error": str(e)}
    
    def is_ready(self) -> bool:
        return self._ready
    
    def get_name(self) -> str:
        return "Tesseract"


class EasyOCREngine(BaseOCREngine):
    """EasyOCR engine implementation."""
    
    def __init__(self, languages: List[str] = None, use_gpu: bool = False, **kwargs):
        self.languages = languages or ["ch_sim", "en"]
        self.use_gpu = use_gpu
        self.reader = None
        self._ready = False
        self._config = kwargs
        
    def initialize(self) -> bool:
        """Initialize EasyOCR engine."""
        try:
            import easyocr
            
            self.reader = easyocr.Reader(
                self.languages,
                gpu=self.use_gpu
            )
            self._ready = True
            logger.info(f"EasyOCR initialized: langs={self.languages}, gpu={self.use_gpu}")
            return True
        except ImportError:
            logger.error("easyocr not installed. Install with: pip install easyocr")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize EasyOCR: {e}")
            return False
    
    def recognize(self, image: Union[bytes, np.ndarray, str], **kwargs) -> Dict[str, Any]:
        """Recognize text using EasyOCR."""
        if not self.is_ready():
            return {"text": "", "boxes": [], "confidence": 0.0, "error": "Engine not ready"}
        
        try:
            # Handle bytes input
            if isinstance(image, bytes):
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    tmp.write(image)
                    tmp_path = tmp.name
                result = self.reader.readtext(tmp_path)
                os.unlink(tmp_path)
            else:
                result = self.reader.readtext(image)
            
            texts = []
            boxes = []
            confidences = []
            
            for detection in result:
                box, text, confidence = detection
                texts.append(text)
                boxes.append(box)
                confidences.append(confidence)
            
            full_text = "\n".join(texts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            return {
                "text": full_text,
                "boxes": boxes,
                "confidences": confidences,
                "confidence": avg_confidence
            }
            
        except Exception as e:
            logger.error(f"EasyOCR recognition error: {e}")
            return {"text": "", "boxes": [], "confidence": 0.0, "error": str(e)}
    
    def is_ready(self) -> bool:
        return self._ready and self.reader is not None
    
    def get_name(self) -> str:
        return "EasyOCR"


class OCREngine:
    """
    Unified OCR Engine interface.
    Supports multiple backends with fallback capability.
    """
    
    ENGINES = {
        "paddleocr": PaddleOCREngine,
        "tesseract": TesseractEngine,
        "easyocr": EasyOCREngine
    }
    
    def __init__(
        self,
        engine: str = "paddleocr",
        language: str = "ch",
        use_gpu: bool = False,
        fallback_engines: List[str] = None,
        **kwargs
    ):
        self.engine_name = engine
        self.language = language
        self.use_gpu = use_gpu
        self.fallback_engines = fallback_engines or []
        self._config = kwargs
        self._engine: Optional[BaseOCREngine] = None
        self._initialized = False
        
    def initialize(self) -> bool:
        """Initialize the OCR engine with fallback support."""
        engines_to_try = [self.engine_name] + self.fallback_engines
        
        for engine_name in engines_to_try:
            if engine_name not in self.ENGINES:
                logger.warning(f"Unknown engine: {engine_name}")
                continue
            
            engine_class = self.ENGINES[engine_name]
            engine_instance = engine_class(
                language=self.language,
                use_gpu=self.use_gpu,
                **self._config
            )
            
            if engine_instance.initialize():
                self._engine = engine_instance
                self._initialized = True
                logger.info(f"Successfully initialized: {engine_name}")
                return True
            
            logger.warning(f"Failed to initialize: {engine_name}")
        
        logger.error("All OCR engines failed to initialize")
        return False
    
    def recognize(self, image: Union[bytes, np.ndarray, str], filename: str = None, **kwargs) -> Dict[str, Any]:
        """
        Recognize text from an image.
        
        Args:
            image: Image data (bytes, numpy array, or file path)
            filename: Optional filename for logging
            **kwargs: Additional arguments passed to the engine
            
        Returns:
            Dictionary containing recognized text and metadata
        """
        if not self.is_ready():
            if not self.initialize():
                return {
                    "text": "",
                    "confidence": 0.0,
                    "error": "OCR engine initialization failed"
                }
        
        result = self._engine.recognize(image, **kwargs)
        
        if filename:
            result["filename"] = filename
            
        return result
    
    def is_ready(self) -> bool:
        """Check if the engine is ready for processing."""
        return self._initialized and self._engine is not None and self._engine.is_ready()
    
    def get_engine_name(self) -> str:
        """Get the name of the currently active engine."""
        return self._engine.get_name() if self._engine else "None"
    
    async def recognize_async(self, image: Union[bytes, np.ndarray, str], filename: str = None, **kwargs) -> Dict[str, Any]:
        """Async wrapper for recognize method."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.recognize(image, filename, **kwargs)
        )


# Module-level singleton for shared usage
_default_engine: Optional[OCREngine] = None


def get_default_engine() -> OCREngine:
    """Get or create the default OCR engine instance."""
    global _default_engine
    if _default_engine is None:
        _default_engine = OCREngine()
        _default_engine.initialize()
    return _default_engine
