from pymongo.mongo_client import MongoClient
from datetime import datetime
from typing import Dict, Any

class MortgageApplicationDB:
    def __init__(self, connection_string: str):
        """Initialize database connection with MongoDB Atlas"""
        self.client = MongoClient(connection_string)
        self.db = self.client.mortgage_db
        self.applications = self.db.applications

        # Create an index on application date for better query performance
        #self.applications.create_index([("application_date", 1)])

    def create_application(self, application_data: Dict[str, Any]) -> str:
        """Create a new mortgage application record"""
        application = {
            "_id": application_data.get("username"),
            "credit": application_data.get("credit"),
            "had_bankrupcies": application_data.get("had_bankrupcies", False),
            "is_working": application_data.get("is_working", False),
            "income": application_data.get("income"),
            "is_veteran": application_data.get("is_veteran", False),
            "first_time_home_buyer": application_data.get("first_time_home_buyer", False),
            "property_type": application_data.get("property_type"),
            "budget": application_data.get("budget"),
            "had_loans_before": application_data.get("had_loans_before", False),
            "home_value": application_data.get("home_value"),
        }
        
        # Validate required fields
        required_fields = ["credit_score", "home_value", "down_payment", 
                         "employment_status", "annual_income", "property_type"]
        
        for field in required_fields:
            if not application.get(field):
                raise ValueError(f"Missing required field: {field}")

        # Insert the application
        result = self.applications.insert_one(application)
        return str(result.inserted_id)

    def get_application(self, application_id: str) -> Dict[str, Any]:
        """Retrieve a specific application by ID"""
        from bson.objectid import ObjectId
        return self.applications.find_one({"_id": ObjectId(application_id)})

    def update_application(self, application_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing application"""
        from bson.objectid import ObjectId
        result = self.applications.update_one(
            {"_id": ObjectId(application_id)},
            {"$set": updates}
        )
        return result.modified_count > 0

    def delete_application(self, application_id: str) -> bool:
        """Delete an application"""
        from bson.objectid import ObjectId
        result = self.applications.delete_one({"_id": ObjectId(application_id)})
        return result.deleted_count > 0

# Example usage
if __name__ == "__main__":
    # Replace with your MongoDB Atlas connection string
    CONNECTION_STRING = "mongodb+srv://user1:wbUjTCvU5o2YQ3Ng@hackutd-project-dev-clu.n76pi.mongodb.net/?retryWrites=true&w=majority&appName=HackUTD-Project-dev-cluster"
    
    # Initialize the database
    db = MortgageApplicationDB(CONNECTION_STRING)
    
    # Example application data
    sample_application = {
        "credit": 720,
        "bankruptcy_history": False,
        "is_veteran": True,
        "home_value": 350000,
        "down_payment": 70000,
        "employment_status": "full_time",
        "annual_income": 85000,
        "first_time_buyer": True,
        "property_type": "single_family"
    }
    
    # Create a new application
    application_id = db.create_application(sample_application)
    print(f"Created application with ID: {application_id}")