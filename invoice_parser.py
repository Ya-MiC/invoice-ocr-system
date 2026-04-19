#!/usr/bin/env python3
"""
Invoice Parser - Extract structured data from OCR text.
Supports Chinese VAT invoices (普通发票, 专用发票, 电子发票).
"""

import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class InvoiceItem:
    """Represents a single invoice line item."""
    name: str = ""
    specification: str = ""
    unit: str = ""
    quantity: Decimal = Decimal("0")
    unit_price: Decimal = Decimal("0")
    amount: Decimal = Decimal("0")
    tax_rate: Decimal = Decimal("0")
    tax_amount: Decimal = Decimal("0")


@dataclass
class InvoiceData:
    """Structured invoice data."""
    # Invoice identification
    invoice_type: str = ""
    invoice_code: str = ""
    invoice_number: str = ""
    
    # Dates
    issue_date: str = ""
    
    # Parties
    seller_name: str = ""
    seller_tax_id: str = ""
    seller_address: str = ""
    seller_phone: str = ""
    seller_bank: str = ""
    seller_account: str = ""
    
    buyer_name: str = ""
    buyer_tax_id: str = ""
    buyer_address: str = ""
    buyer_phone: str = ""
    buyer_bank: str = ""
    buyer_account: str = ""
    
    # Financials
    currency: str = "CNY"
    items: List[InvoiceItem] = field(default_factory=list)
    subtotal: Decimal = Decimal("0")
    tax_total: Decimal = Decimal("0")
    total: Decimal = Decimal("0")
    
    # Additional info
    remarks: str = ""
    payee: str = ""
    reviewer: str = ""
    drawer: str = ""
    
    # Metadata
    raw_text: str = ""
    confidence: float = 0.0
    parse_errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["items"] = [asdict(item) for item in self.items]
        data["subtotal"] = str(self.subtotal)
        data["tax_total"] = str(self.tax_total)
        data["total"] = str(self.total)
        return data


class InvoiceParser:
    """
    Production-grade invoice parser for Chinese VAT invoices.
    Uses regex patterns and heuristics to extract structured data.
    """
    
    # Common invoice type patterns
    INVOICE_TYPES = {
        "增值税电子普通发票": "electronic_normal",
        "增值税普通发票": "normal",
        "增值税专用发票": "special",
        "增值税电子专用发票": "electronic_special",
        "机动车销售统一发票": "vehicle",
        "二手车销售统一发票": "secondhand_vehicle"
    }
    
    def __init__(self):
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Pre-compile regex patterns for performance."""
        
        # Invoice code and number
        self.pattern_code = re.compile(
            r"发票代码[：:]\s*(\d{10,12})|代码[：:]\s*(\d{10,12})",
            re.IGNORECASE
        )
        self.pattern_number = re.compile(
            r"发票号码[：:]\s*(\d{8,20})|号码[：:]\s*(\d{8,20})",
            re.IGNORECASE
        )
        
        # Dates
        self.pattern_date = re.compile(
            r"开票日期[：:]\s*(\d{4}[/年\-\.]\d{1,2}[/月\-\.]\d{1,2}日?)",
            re.IGNORECASE
        )
        
        # Seller information
        self.pattern_seller = re.compile(
            r"(?:销售方|收款人|开票方)[信息]?[：:]?\s*名称[：:]\s*(.+?)(?=\s*纳税人|税号|$)",
            re.IGNORECASE | re.DOTALL
        )
        self.pattern_seller_tax_id = re.compile(
            r"(?:销售方|收款人|开票方)[\s\S]*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
            re.IGNORECASE
        )
        
        # Buyer information  
        self.pattern_buyer = re.compile(
            r"(?:购买方|购货方|付款人|受票方)[信息]?[：:]?\s*名称[：:]\s*(.+?)(?=\s*纳税人|税号|$)",
            re.IGNORECASE | re.DOTALL
        )
        self.pattern_buyer_tax_id = re.compile(
            r"(?:购买方|购货方|付款人|受票方)[\s\S]*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
            re.IGNORECASE
        )
        
        # Financial amounts
        self.pattern_amounts = re.compile(
            r"(?:合\s*计|小\s*计)[：:]\s*[\￥¥]?\s*([0-9,]+\.?[0-9]*)",
            re.IGNORECASE
        )
        self.pattern_tax = re.compile(
            r"(?:税\s*额|税额)[：:]\s*[\￥¥]?\s*([0-9,]+\.?[0-9]*)",
            re.IGNORECASE
        )
        self.pattern_total = re.compile(
            r"(?:价税合计|总\s*计|合计金额)[（(]大写[）)]?[：:]\s*[\￥¥]?\s*([0-9,]+\.?[0-9]*)",
            re.IGNORECASE
        )
        
        # Item lines pattern (for tabular invoice formats)
        self.pattern_items = re.compile(
            r"([^
]+?)\s+(\d+\.?\d*)\s+[\￥¥]?\s*([0-9,]+\.?[0-9]*)\s+(\d+%?)\s+[\￥¥]?\s*([0-9,]+\.?[0-9]*)",
            re.MULTILINE
        )
        
        # Remarks
        self.pattern_remarks = re.compile(
            r"备注[：:]\s*(.+?)(?=
\s*
|
\s*(?:收款|复核|开票)|$)",
            re.IGNORECASE | re.DOTALL
        )
        
        # Personnel
        self.pattern_payee = re.compile(r"收款人[：:]\s*(.+?)(?=
|$)", re.IGNORECASE)
        self.pattern_reviewer = re.compile(r"复核人?[：:]\s*(.+?)(?=
|$)", re.IGNORECASE)
        self.pattern_drawer = re.compile(r"(?:开票人|开票单位)[：:]\s*(.+?)(?=
|$)", re.IGNORECASE)
        
    def parse(self, ocr_text: str) -> InvoiceData:
        """
        Parse OCR text and extract invoice data.
        
        Args:
            ocr_text: Raw text from OCR recognition
            
        Returns:
            InvoiceData object with extracted fields
        """
        invoice = InvoiceData(raw_text=ocr_text)
        
        # Normalize text
        text = self._normalize_text(ocr_text)
        
        # Extract invoice type
        invoice.invoice_type = self._extract_invoice_type(text)
        
        # Extract basic info
        invoice.invoice_code = self._extract_pattern_first(self.pattern_code, text)
        invoice.invoice_number = self._extract_pattern_first(self.pattern_number, text)
        invoice.issue_date = self._extract_date(text)
        
        # Extract party information
        self._extract_seller_info(text, invoice)
        self._extract_buyer_info(text, invoice)
        
        # Extract financial data
        self._extract_amounts(text, invoice)
        
        # Extract items
        invoice.items = self._extract_items(text)
        
        # Extract additional info
        invoice.remarks = self._extract_pattern_first(self.pattern_remarks, text)
        invoice.payee = self._extract_pattern_first(self.pattern_payee, text)
        invoice.reviewer = self._extract_pattern_first(self.pattern_reviewer, text)
        invoice.drawer = self._extract_pattern_first(self.pattern_drawer, text)
        
        # Validate and clean
        self._validate_invoice(invoice)
        
        return invoice
    
    def _normalize_text(self, text: str) -> str:
        """Normalize OCR text for better parsing."""
        # Replace common OCR errors
        replacements = {
            "：": ":",
            "　": " ",
            "	": " ",
            "　": " ",
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Remove extra whitespace but preserve line breaks
        lines = text.split("\n")
        lines = [" ".join(line.split()) for line in lines]
        
        return "\n".join(lines)
    
    def _extract_invoice_type(self, text: str) -> str:
        """Identify invoice type from text."""
        for type_name, type_code in self.INVOICE_TYPES.items():
            if type_name in text:
                return type_code
        
        # Try to infer from content
        if "增值税" in text:
            if "专用" in text:
                return "special"
            elif "普通" in text:
                return "normal"
        
        return "unknown"
    
    def _extract_pattern_first(self, pattern: re.Pattern, text: str) -> str:
        """Extract first match from pattern."""
        match = pattern.search(text)
        if match:
            # Return first non-None group
            for group in match.groups():
                if group:
                    return group.strip()
        return ""
    
    def _extract_date(self, text: str) -> str:
        """Extract and normalize date."""
        match = self.pattern_date.search(text)
        if match:
            date_str = match.group(1)
            # Normalize date format
            date_str = re.sub(r"[年月]", "-", date_str)
            date_str = re.sub(r"日", "", date_str)
            date_str = date_str.replace("/", "-")
            return date_str
        return ""
    
    def _extract_seller_info(self, text: str, invoice: InvoiceData):
        """Extract seller information."""
        # Extract name
        match = self.pattern_seller.search(text)
        if match:
            invoice.seller_name = match.group(1).strip()
        
        # Extract tax ID
        match = self.pattern_seller_tax_id.search(text)
        if match:
            invoice.seller_tax_id = match.group(1).strip()
        
        # Extract address and phone (often in same line)
        addr_pattern = re.compile(
            r"(?:销售方[\s\S]*?)地址[：:]\s*(.+?)\s*(?:电话|账号|$)",
            re.IGNORECASE
        )
        match = addr_pattern.search(text)
        if match:
            invoice.seller_address = match.group(1).strip()
        
        # Extract bank and account
        bank_pattern = re.compile(
            r"(?:销售方[\s\S]*?)开户行[：:]\s*(.+?)\s*账号[：:]\s*(.+?)(?=
|$)",
            re.IGNORECASE
        )
        match = bank_pattern.search(text)
        if match:
            invoice.seller_bank = match.group(1).strip()
            invoice.seller_account = match.group(2).strip()
    
    def _extract_buyer_info(self, text: str, invoice: InvoiceData):
        """Extract buyer information."""
        # Extract name
        match = self.pattern_buyer.search(text)
        if match:
            invoice.buyer_name = match.group(1).strip()
        
        # Extract tax ID
        match = self.pattern_buyer_tax_id.search(text)
        if match:
            invoice.buyer_tax_id = match.group(1).strip()
        
        # Extract address
        addr_pattern = re.compile(
            r"(?:购买方[\s\S]*?)地址[：:]\s*(.+?)\s*(?:电话|账号|$)",
            re.IGNORECASE
        )
        match = addr_pattern.search(text)
        if match:
            invoice.buyer_address = match.group(1).strip()
        
        # Extract bank and account
        bank_pattern = re.compile(
            r"(?:购买方[\s\S]*?)开户行[：:]\s*(.+?)\s*账号[：:]\s*(.+?)(?=
|$)",
            re.IGNORECASE
        )
        match = bank_pattern.search(text)
        if match:
            invoice.buyer_bank = match.group(1).strip()
            invoice.buyer_account = match.group(2).strip()
    
    def _extract_amounts(self, text: str, invoice: InvoiceData):
        """Extract financial amounts."""
        # Extract subtotal (amount without tax)
        amounts = self.pattern_amounts.findall(text)
        if amounts:
            try:
                invoice.subtotal = self._parse_decimal(amounts[0])
            except (ValueError, IndexError):
                pass
        
        # Extract tax
        taxes = self.pattern_tax.findall(text)
        if taxes:
            try:
                invoice.tax_total = self._parse_decimal(taxes[0])
            except (ValueError, IndexError):
                pass
        
        # Extract total
        totals = self.pattern_total.findall(text)
        if totals:
            try:
                invoice.total = self._parse_decimal(totals[0])
            except (ValueError, IndexError):
                pass
        
        # Calculate missing values
        if invoice.total and not invoice.subtotal:
            if invoice.tax_total:
                invoice.subtotal = invoice.total - invoice.tax_total
        elif invoice.subtotal and not invoice.total:
            if invoice.tax_total:
                invoice.total = invoice.subtotal + invoice.tax_total
    
    def _extract_items(self, text: str) -> List[InvoiceItem]:
        """Extract line items from invoice."""
        items = []
        
        # Try tabular format first
        matches = self.pattern_items.findall(text)
        
        for match in matches:
            try:
                item = InvoiceItem(
                    name=match[0].strip() if len(match) > 0 else "",
                    quantity=self._parse_decimal(match[1]) if len(match) > 1 else Decimal("0"),
                    unit_price=self._parse_decimal(match[2]) if len(match) > 2 else Decimal("0"),
                    tax_rate=self._parse_tax_rate(match[3]) if len(match) > 3 else Decimal("0"),
                    tax_amount=self._parse_decimal(match[4]) if len(match) > 4 else Decimal("0")
                )
                # Calculate amount if not given
                if item.quantity and item.unit_price:
                    item.amount = item.quantity * item.unit_price
                items.append(item)
            except Exception as e:
                logger.debug(f"Failed to parse item: {e}")
        
        return items
    
    def _parse_decimal(self, value: str) -> Decimal:
        """Parse string to Decimal, handling various formats."""
        # Remove currency symbols and spaces
        value = re.sub(r"[￥¥$\s,]", "", value.strip())
        
        if not value:
            return Decimal("0")
        
        return Decimal(value)
    
    def _parse_tax_rate(self, value: str) -> Decimal:
        """Parse tax rate string to Decimal."""
        value = value.strip().replace("%", "")
        
        if not value:
            return Decimal("0")
        
        try:
            rate = Decimal(value)
            # If rate > 1, it's a percentage
            if rate > 1:
                rate = rate / 100
            return rate
        except:
            return Decimal("0")
    
    def _validate_invoice(self, invoice: InvoiceData):
        """Validate extracted data and flag issues."""
        errors = []
        
        if not invoice.invoice_number:
            errors.append("Missing invoice number")
        
        if not invoice.seller_name:
            errors.append("Missing seller name")
        
        if not invoice.buyer_name:
            errors.append("Missing buyer name")
        
        # Validate amounts
        if invoice.subtotal and invoice.tax_total and invoice.total:
            calculated_total = invoice.subtotal + invoice.tax_total
            if abs(calculated_total - invoice.total) > Decimal("0.01"):
                errors.append(f"Amount mismatch: subtotal({invoice.subtotal}) + tax({invoice.tax_total}) != total({invoice.total})")
        
        invoice.parse_errors = errors
        
        if errors:
            logger.warning(f"Invoice validation issues: {errors}")
    
    def to_csv_row(self, invoice: InvoiceData) -> Dict[str, str]:
        """Convert invoice to flat CSV row format."""
        return {
            "invoice_type": invoice.invoice_type,
            "invoice_code": invoice.invoice_code,
            "invoice_number": invoice.invoice_number,
            "issue_date": invoice.issue_date,
            "seller_name": invoice.seller_name,
            "seller_tax_id": invoice.seller_tax_id,
            "buyer_name": invoice.buyer_name,
            "buyer_tax_id": invoice.buyer_tax_id,
            "subtotal": str(invoice.subtotal),
            "tax_total": str(invoice.tax_total),
            "total": str(invoice.total),
            "currency": invoice.currency,
            "payee": invoice.payee,
            "reviewer": invoice.reviewer,
            "drawer": invoice.drawer,
            "remarks": invoice.remarks[:200] if invoice.remarks else ""
        }
