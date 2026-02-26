"""
Firebase State Manager for Neuro-Adaptive Trading Ecosystem
Manages real-time state, model persistence, and self-healing coordination
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import firebase_admin
from firebase_admin import credentials, firestore, exceptions
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FirebaseStateManager:
    """Centralized state management using Firestore with self-healing capabilities"""
    
    def __init__(self, credential_path: str = None):
        """
        Initialize Firebase connection with proper error handling
        
        Args:
            credential_path: Path to Firebase service account JSON file
        """
        self.db = None
        self.initialized = False
        
        try:
            if not firebase_admin._apps:
                if credential_path and os.path.exists(credential_path):
                    cred = credentials.Certificate(credential_path)
                else:
                    # Try environment variable
                    cred_json = os.getenv('FIREBASE_CREDENTIALS')
                    if cred_json:
                        cred_dict = json.loads(cred_json)
                        cred = credentials.Certificate(cred_dict)
                    else:
                        # Try default path
                        default_path = 'config/firebase_credentials.json'
                        if os.path.exists(default_path):
                            cred = credentials.Certificate(default_path)
                        else:
                            raise FileNotFoundError(
                                "No Firebase credentials found. Please provide credentials via: "
                                "1) credential_path parameter, 2) FIREBASE_CREDENTIALS env var, "
                                "3) config/firebase_credentials.json"
                            )
                
                firebase_admin.initialize_app(cred)
                logger.info("Firebase Admin SDK initialized successfully")
            
            self.db = firestore.client()
            self.initialized = True
            logger.info("Firestore client connected")
            
            # Initialize collections if they don't exist
            self._initialize_collections()
            
        except exceptions.FirebaseError as e:
            logger.error(f"Firebase initialization error: {e}")
            self.initialized = False
        except Exception as e:
            logger.error(f"Unexpected initialization error: {e}")
            self.initialized = False
    
    def _initialize_collections(self):
        """Ensure required collections exist with proper structure"""
        required_collections = [
            'trading_state',
            'neural_models',
            'anomaly_logs',
            'healing_actions',
            'performance_metrics'
        ]
        
        for collection in required_collections:
            try:
                # Create a test document to ensure collection exists
                test_ref = self.db.collection(collection).document('_initialization')
                test_ref.set({
                    'initialized_at': datetime.utcnow().isoformat(),
                    'purpose': 'Collection initialization marker'
                }, merge=True)
                logger.debug(f"Collection '{collection}' initialized")
            except Exception as e:
                logger.warning(f"Could not initialize collection '{collection}': {e}")
    
    def save_trading_state(self, state: Dict[str, Any]) -> bool:
        """
        Save current trading state with versioning
        
        Args:
            state: Trading state dictionary
            
        Returns:
            bool: Success status
        """
        if not self.initialized:
            logger.error("Firebase not initialized")
            return False
        
        try:
            # Add metadata
            state_with_meta = {
                **state,
                'timestamp': datetime.utcnow().isoformat(),
                'version': state.get('version', '1.0.0'),
                'last_updated': firestore.SERVER_TIMESTAMP
            }
            
            # Save to Firestore
            doc_ref = self.db.collection('trading_state').document('current')
            doc_ref.set(state_with_meta, merge=True)
            
            # Also create versioned backup
            backup_ref = self.db.collection('trading_state_history').document(
                f"state_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            )
            backup_ref.set(state_with_meta)
            
            logger.info(f"Trading state saved successfully: {len(state)} fields")
            return True
            
        except exceptions.FirebaseError as e:
            logger.error(f"Firebase error saving trading state: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving trading state: {e}")
            return False
    
    def load_trading_state(self) -> Optional[Dict[str, Any]]:
        """Load current trading state"""
        if not self.initialized:
            logger.error("Firebase not initialized")
            return None
        
        try:
            doc_ref = self.db.collection('trading_state').document('current')
            doc = doc_ref.get()
            
            if doc.exists:
                state = doc.to_dict()
                logger.info("Trading state loaded successfully")
                return state
            else:
                logger.w