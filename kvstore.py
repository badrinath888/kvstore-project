class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.
    
    • Commands: SET <key> <value>, GET <key>, EXIT
    • Storage: data.db (append-only log)
    • Last-write-wins semantics
    """

    def __init__(self) -> None:
        """Initialize the key-value store by replaying the log into memory."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Load key-value pairs from the append-only log file (data.db).
        
        Rebuilds the in-memory index by replaying all logged SET operations.
        Skips malformed lines gracefully.
        """
        ...

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair persistently.
        
        Appends a log entry to `data.db` and updates the in-memory index.
        Ensures durability by flushing and syncing the file to disk.
        """
        ...

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the latest value for a given key.
        
        Returns:
            str: The stored value if found.
            None: If the key does not exist.
        """
        ...



