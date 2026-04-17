# Foundation SAR - Core Data Schemas and Utilities
# TODO: Implement core Pydantic schemas and data processing utilities

"""
This module contains the foundational components for SAR processing:

1. Pydantic Data Schemas:
   - CustomerData: Customer profile information
   - AccountData: Account details and balances  
   - TransactionData: Individual transaction records
   - CaseData: Unified case combining all data sources
   - RiskAnalystOutput: Risk analysis results
   - ComplianceOfficerOutput: Compliance narrative results

2. Utility Classes:
   - ExplainabilityLogger: Audit trail logging
   - DataLoader: Combines fragmented data into case objects

YOUR TASKS:
- Study the data files in data/ folder
- Design Pydantic schemas that match the CSV structure
- Implement validation rules for financial data
- Create a DataLoader that builds unified case objects
- Add proper error handling and logging
"""

import json
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, field_validator
import uuid
import os

# ===== TODO: IMPLEMENT PYDANTIC SCHEMAS =====

class CustomerData(BaseModel):
    """Customer information schema with validation
    
    REQUIRED FIELDS (examine data/customers.csv):
    - customer_id: str = Unique identifier like "CUST_0001"
    - name: str = Full customer name like "John Smith"
    - date_of_birth: str = Date in YYYY-MM-DD format like "1985-03-15"
    - ssn_last_4: str = Last 4 digits like "1234"
    - address: str = Full address like "123 Main St, City, ST 12345"
    - customer_since: str = Date in YYYY-MM-DD format like "2010-01-15"
    - risk_rating: Literal['Low', 'Medium', 'High'] = Risk assessment
    
    OPTIONAL FIELDS:
    - phone: Optional[str] = Phone number like "555-123-4567"
    - occupation: Optional[str] = Job title like "Software Engineer"
    - annual_income: Optional[int] = Yearly income like 75000
    
    HINT: Use Field(..., description="...") for required fields
    HINT: Use Field(None, description="...") for optional fields
    HINT: Use Literal type for risk_rating to restrict values
    """
    # --- Core identifiers ---
    customer_id: str = Field(..., description="Unique customer identifier e.g. CUST_0001")
    name: str = Field(..., description="Full customer name")
 
    # --- Dates ---
    date_of_birth: str = Field(..., description="Date of birth in YYYY-MM-DD format")
    customer_since: str = Field(..., description="Account opening date in YYYY-MM-DD format")
 
    # --- Privacy ---
    ssn_last_4: str = Field(..., description="Last 4 digits of SSN only — never store full SSN")
 
    # --- Contact ---
    address: str = Field(..., description="Full customer address")
    phone: Optional[str] = Field(None, description="Customer phone number")
 
    # --- Risk & Profile ---
    risk_rating: Literal["Low", "Medium", "High"] = Field(
        ..., description="AML risk rating"
    )
    occupation: Optional[str] = Field(None, description="Customer occupation")
    annual_income: Optional[int] = Field(None, ge=0, description="Annual income in USD")
 
    @field_validator("ssn_last_4", mode="before")
    @classmethod
    def coerce_ssn(cls, v):
        """CSV stores ssn_last_4 as int64, convert to zero-padded string"""
        return str(v).zfill(4)
 
    @field_validator("risk_rating", mode="before")
    @classmethod
    def normalize_risk_rating(cls, v):
        return v.strip().capitalize() if isinstance(v, str) else v
    

    # TODO: Implement the CustomerData schema with proper fields and validation
    pass

class AccountData(BaseModel):
    """Account information schema with validation
    
    REQUIRED FIELDS (examine data/accounts.csv):
    - account_id: str = Unique identifier like "CUST_0001_ACC_1"
    - customer_id: str = Must match CustomerData.customer_id
    - account_type: str = Type like "Checking", "Savings", "Money_Market"
    - opening_date: str = Date in YYYY-MM-DD format
    - current_balance: float = Current balance (can be negative)
    - average_monthly_balance: float = Average balance
    - status: str = Status like "Active", "Closed", "Suspended"
    
    HINT: All fields are required for account data
    HINT: Use float for monetary amounts
    HINT: current_balance can be negative for overdrafts
    """
# --- Identifiers & linking ---
    account_id: str = Field(..., description="Unique account identifier")
    customer_id: str = Field(..., description="FK → links to CustomerData.customer_id")
 
    # --- Account type & status ---
    account_type: str = Field(..., description="Type of account e.g. Checking, Savings, Money_Market")
    status: str = Field(..., description="Account status e.g. Active, Closed, Suspended")
 
    # --- Dates ---
    opening_date: str = Field(..., description="Account opening date in YYYY-MM-DD format")
 
    # --- Balances ---
    current_balance: float = Field(..., description="Current balance in USD (can be negative for overdraft)")
    average_monthly_balance: float = Field(..., ge=0, description="Average monthly balance in USD")
 
    @field_validator("account_type", "status", mode="before")
    @classmethod
    def normalize_strings(cls, v):
        return v.strip() if isinstance(v, str) else v

    # TODO: Implement the AccountData schema
    pass

class TransactionData(BaseModel):
    """Transaction information schema with validation
    
    REQUIRED FIELDS (examine data/transactions.csv):
    - transaction_id: str = Unique identifier like "TXN_B24455F3"
    - account_id: str = Must match AccountData.account_id
    - transaction_date: str = Date in YYYY-MM-DD format
    - transaction_type: str = Type like "Cash_Deposit", "Wire_Transfer"
    - amount: float = Transaction amount (negative for withdrawals)
    - description: str = Description like "Cash deposit at branch"
    - method: str = Method like "Wire", "ACH", "ATM", "Teller"
    
    OPTIONAL FIELDS:
    - counterparty: Optional[str] = Other party in transaction
    - location: Optional[str] = Transaction location or branch
    
    HINT: amount can be negative for debits/withdrawals
    HINT: Use descriptive field descriptions for clarity
    """

    # --- Identifiers & linking ---
    transaction_id: str = Field(..., description="Unique transaction identifier")
    account_id: str = Field(..., description="FK → links to AccountData.account_id")
 
    # --- Date ---
    transaction_date: str = Field(..., description="Transaction date in YYYY-MM-DD format")
 
    # --- Transaction details ---
    transaction_type: str = Field(..., description="Type of transaction e.g. Cash_Deposit, Wire_Transfer")
    amount: float = Field(..., description="Transaction amount — negative for withdrawals/debits")
    method: str = Field(..., description="Transaction method e.g. Wire, ACH, ATM, Teller")
 
    # --- Descriptive / optional ---
    description: str = Field(..., description="Transaction description")
    counterparty: Optional[str] = Field(None, description="Counterparty name if applicable")
    location: Optional[str] = Field(None, description="Branch or location if applicable")
 
    @field_validator("transaction_type", "method", mode="before")
    @classmethod
    def normalize_strings(cls, v):
        return v.strip() if isinstance(v, str) else v
 
    @field_validator("counterparty", "location", mode="before")
    @classmethod
    def handle_nan(cls, v):
        """Convert NaN values from CSV to None"""
        if v != v:  # NaN check (NaN != NaN is True)
            return None
        return v if v else None
  
    
    # TODO: Implement the TransactionData schema
    pass

class CaseData(BaseModel):
    """Unified case object combining all data sources
    
    REQUIRED FIELDS:
    - case_id: str = Unique case identifier (generate with uuid)
    - customer: CustomerData = Customer information object
    - accounts: List[AccountData] = List of customer's accounts
    - transactions: List[TransactionData] = List of suspicious transactions
    - case_created_at: str = ISO timestamp when case was created
    - data_sources: Dict[str, str] = Source tracking with keys like:
      * "customer_source": "csv_extract_20241219"
      * "account_source": "csv_extract_20241219" 
      * "transaction_source": "csv_extract_20241219"
    
    VALIDATION RULES:
    - transactions list cannot be empty (use @field_validator)
    - All accounts should belong to the same customer
    - All transactions should belong to accounts in the case
    
    HINT: Use @field_validator('transactions') with @classmethod decorator
    HINT: Check if not v: raise ValueError("message") for empty validation
    """

    # --- Identifiers ---
    case_id: str = Field(..., description="Unique case identifier generated with uuid")
 
    # --- Data objects ---
    customer: CustomerData = Field(..., description="Customer information object")
    accounts: List[AccountData] = Field(..., description="List of customer accounts")
    transactions: List[TransactionData] = Field(..., description="List of transactions")
 
    # --- Metadata ---
    case_created_at: str = Field(..., description="ISO timestamp when case was created")
    data_sources: Dict[str, str] = Field(..., description="Source tracking dictionary")
 
    @field_validator("transactions")
    @classmethod
    def transactions_not_empty(cls, v):
        if not v:
            raise ValueError("Transactions list cannot be empty — no case without transactions")
        return v
 
    @field_validator("accounts")
    @classmethod
    def accounts_not_empty(cls, v):
        if not v:
            raise ValueError("Accounts list cannot be empty")
        return v
    
    # TODO: Implement the CaseData schema with validation
    pass


class RiskAnalystOutput(BaseModel):
    """Risk Analyst agent structured output
    
    REQUIRED FIELDS (for Chain-of-Thought agent output):
    - classification: Literal['Structuring', 'Sanctions', 'Fraud', 'Money_Laundering', 'Other']
    - confidence_score: float = Confidence between 0.0 and 1.0 (use ge=0.0, le=1.0)
    - reasoning: str = Step-by-step analysis reasoning (max 500 chars)
    - key_indicators: List[str] = List of suspicious indicators found
    - risk_level: Literal['Low', 'Medium', 'High', 'Critical'] = Risk assessment
    
    HINT: Use Literal types to restrict classification and risk_level values
    HINT: Use Field(..., ge=0.0, le=1.0) for confidence_score validation
    HINT: Use Field(..., max_length=500) for reasoning length limit
    """
 
    classification: Literal[
        "Structuring", "Sanctions", "Fraud", "Money_Laundering", "Other"
    ] = Field(..., description="Type of suspicious activity detected")
 
    confidence_score: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0"
    )
 
    reasoning: str = Field(
        ..., max_length=500, description="Step-by-step analysis reasoning"
    )
 
    key_indicators: List[str] = Field(
        ..., description="List of suspicious indicators found"
    )
 
    risk_level: Literal["Low", "Medium", "High", "Critical"] = Field(
        ..., description="Overall risk assessment"
    )
    # TODO: Implement the RiskAnalystOutput schema
    pass

class ComplianceOfficerOutput(BaseModel):
    """Compliance Officer agent structured output
    
    REQUIRED FIELDS (for ReACT agent output):
    - narrative: str = Regulatory narrative text (max 1000 chars for ≤200 words)
    - narrative_reasoning: str = Reasoning for narrative construction (max 500 chars)
    - regulatory_citations: List[str] = List of relevant regulations like:
      * "31 CFR 1020.320 (BSA)"
      * "12 CFR 21.11 (SAR Filing)"
      * "FinCEN SAR Instructions"
    - completeness_check: bool = Whether narrative meets all requirements
    
    HINT: Use Field(..., max_length=1000) for narrative length limit
    HINT: Use Field(..., max_length=500) for reasoning length limit
    HINT: Use bool type for completeness_check
    """
    """Compliance Officer agent structured output"""
 
    narrative: str = Field(
        ..., max_length=1000, description="Regulatory narrative text (max ~200 words)"
    )
 
    narrative_reasoning: str = Field(
        ..., max_length=500, description="Reasoning for narrative construction"
    )
 
    regulatory_citations: List[str] = Field(
        ..., description="List of relevant regulations e.g. '31 CFR 1020.320 (BSA)'"
    )
 
    completeness_check: bool = Field(
        ..., description="Whether the narrative meets all regulatory requirements"
    )
    # TODO: Implement the ComplianceOfficerOutput schema
    pass

# ===== TODO: IMPLEMENT AUDIT LOGGING =====

class ExplainabilityLogger:
    """Simple audit logging for compliance trails

    ATTRIBUTES:
    - log_file: str = Path to JSONL log file (default: "sar_audit.jsonl")
    - entries: List = In-memory storage of log entries

    METHODS:
    - log_agent_action(): Logs agent actions with structured data
    
    LOG ENTRY STRUCTURE (use this exact format):
    {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'case_id': case_id,
        'agent_type': agent_type,  # "DataLoader", "RiskAnalyst", "ComplianceOfficer"
        'action': action,          # "create_case", "analyze_case", "generate_narrative"
        'input_summary': str(input_data),
        'output_summary': str(output_data),
        'reasoning': reasoning,
        'execution_time_ms': execution_time_ms,
        'success': success,        # True/False
        'error_message': error_message  # None if success=True
    }
    
    HINT: Write each entry as JSON + newline to create JSONL format
    HINT: Use 'a' mode to append to log file
    HINT: Store entries in self.entries list AND write to file
    """
    
    def __init__(self, log_file: str = "sar_audit.jsonl"):
        self.log_file = log_file
        self.entries: List[Dict]= []
        
        # TODO: Initialize with log_file path and empty entries list
        pass
    
    def log_agent_action(
        self,
        agent_type: str,
        action: str,
        case_id: str,
        input_data: Dict,
        output_data: Dict,
        reasoning: str,
        execution_time_ms: float,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """Log an agent action with essential context
        
        IMPLEMENTATION STEPS:
        1. Create entry dictionary with all fields (see structure above)
        2. Add entry to self.entries list
        3. Write entry to log file as JSON line
         HINT: Use json.dumps(entry) + '\n' for JSONL format
        HINT: Use datetime.now(timezone.utc).isoformat() for timestamp
        HINT: Convert input_data and output_data to strings with str()
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "agent_type": agent_type,
            "action": action,
            "input_summary": str(input_data),
            "output_summary": str(output_data),
            "reasoning": reasoning,
            "execution_time_ms": execution_time_ms,
            "success": success,
            "error_message": error_message
        }
 
        # Store in memory
        self.entries.append(entry)
 
        # Write to JSONL file
        with open(self.log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

       
        # TODO: Implement logging with structured entry creation and file writing
        pass

# ===== TODO: IMPLEMENT DATA LOADER =====

class DataLoader:
    """Simple loader that creates case objects from CSV data
    
    ATTRIBUTES:
    - logger: ExplainabilityLogger = For audit logging
    
    HELPFUL METHODS:
    - create_case_from_data(): Creates CaseData from input dictionaries
    
    IMPLEMENTATION PATTERN:
    1. Start timing with start_time = datetime.now()
    2. Generate case_id with str(uuid.uuid4())
    3. Create CustomerData object from customer_data dict
    4. Filter accounts where acc['customer_id'] == customer.customer_id
    5. Get account_ids set from filtered accounts
    6. Filter transactions where txn['account_id'] in account_ids
    7. Create CaseData object with all components
    8. Calculate execution_time_ms
    9. Log success/failure with self.logger.log_agent_action()
    10. Return CaseData object (or raise exception on failure)
    """
    
    def __init__(self, explainability_logger: ExplainabilityLogger):
        self.logger = explainability_logger
 
    def create_case_from_data(
        self,
        customer_data: Dict,
        account_data: List[Dict],
        transaction_data: List[Dict]
    ) -> CaseData:
        """Create a unified case object from fragmented AML data

        SUGGESTED STEPS:
        1. Record start time for performance tracking
        2. Generate unique case_id using uuid.uuid4()
        3. Create CustomerData object from customer_data dictionary
        4. Filter account_data list for accounts belonging to this customer
        5. Create AccountData objects from filtered accounts
        6. Get set of account_ids from customer's accounts
        7. Filter transaction_data for transactions in customer's accounts
        8. Create TransactionData objects from filtered transactions  
        9. Create CaseData object combining all components
        10. Add case metadata (case_id, timestamp, data_sources)
        11. Calculate execution time in milliseconds
        12. Log operation with success/failure status
        13. Return CaseData object
        
        ERROR HANDLING:
        - Wrap in try/except block
        - Log failures with error message
        - Re-raise exceptions for caller
        
        DATA_SOURCES FORMAT:
        {
            'customer_source': f"csv_extract_{datetime.now().strftime('%Y%m%d')}",
            'account_source': f"csv_extract_{datetime.now().strftime('%Y%m%d')}",
            'transaction_source': f"csv_extract_{datetime.now().strftime('%Y%m%d')}"
        }
        
        HINT: Use list comprehensions for filtering
        HINT: Use set comprehension for account_ids: {acc.account_id for acc in accounts}
        HINT: Use datetime.now(timezone.utc).isoformat() for timestamps
        HINT: Calculate execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        """
        start_time = datetime.now()
        case_id = str(uuid.uuid4())
 
        try:
            # 1. Build CustomerData object
            customer = CustomerData(**customer_data)
 
            # 2. Filter accounts belonging to this customer
            customer_accounts = [
                AccountData(**acc)
                for acc in account_data
                if acc["customer_id"] == customer.customer_id
            ]
 
            # 3. Get set of account_ids for this customer
            account_ids = {acc.account_id for acc in customer_accounts}
 
            # 4. Filter transactions belonging to customer's accounts
            customer_transactions = [
                TransactionData(**txn)
                for txn in transaction_data
                if txn["account_id"] in account_ids
            ]
 
            # 5. Build CaseData object
            case = CaseData(
                case_id=case_id,
                customer=customer,
                accounts=customer_accounts,
                transactions=customer_transactions,
                case_created_at=datetime.now(timezone.utc).isoformat(),
                data_sources={
                    "customer_source": f"csv_extract_{datetime.now().strftime('%Y%m%d')}",
                    "account_source": f"csv_extract_{datetime.now().strftime('%Y%m%d')}",
                    "transaction_source": f"csv_extract_{datetime.now().strftime('%Y%m%d')}"
                }
            )
 
            # 6. Calculate execution time and log success
            execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
 
            self.logger.log_agent_action(
                agent_type="DataLoader",
                action="create_case",
                case_id=case_id,
                input_data={"customer_id": customer.customer_id},
                output_data={
                    "accounts_count": len(customer_accounts),
                    "transactions_count": len(customer_transactions)
                },
                reasoning=f"Built case for customer {customer.customer_id} with "
                          f"{len(customer_accounts)} accounts and {len(customer_transactions)} transactions",
                execution_time_ms=execution_time_ms,
                success=True
            )
 
            return case
 
        except Exception as e:
            execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
 
            self.logger.log_agent_action(
                agent_type="DataLoader",
                action="create_case",
                case_id=case_id,
                input_data={"customer_id": customer_data.get("customer_id", "unknown")},
                output_data={},
                reasoning="Case creation failed",
                execution_time_ms=execution_time_ms,
                success=False,
                error_message=str(e)
            )
 
            raise


        # TODO: Implement complete case creation with error handling and logging
        pass

# ===== HELPER FUNCTIONS (PROVIDED) =====

def load_csv_data(data_dir: str = "data/") -> tuple:
    """Helper function to load all CSV files
    
    Returns:
        tuple: (customers_df, accounts_df, transactions_df)
    """
    try:
        customers_df = pd.read_csv(f"{data_dir}/customers.csv")
        accounts_df = pd.read_csv(f"{data_dir}/accounts.csv") 
        transactions_df = pd.read_csv(f"{data_dir}/transactions.csv")
        return customers_df, accounts_df, transactions_df
    except FileNotFoundError as e:
        raise FileNotFoundError(f"CSV file not found: {e}")
    except Exception as e:
        raise Exception(f"Error loading CSV data: {e}")

if __name__ == "__main__":
    print("🏗️  Foundation SAR Module")
    print("Core data schemas and utilities for SAR processing")
    print("\n📋 TODO Items:")
    print("• Implement Pydantic schemas based on CSV data")
    print("• Create ExplainabilityLogger for audit trails")
    print("• Build DataLoader for case object creation")
    print("• Add comprehensive error handling")
    print("• Write unit tests for all components")
