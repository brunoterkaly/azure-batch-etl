from abc import ABC, abstractmethod


class DBManagerInterface(ABC):
    @abstractmethod
    def __init__(self):
        """Init"""

    @abstractmethod
    def connect_to_db(self):
        """Connect to db"""

    @abstractmethod
    def create_table(self, cursor):
        """Create table"""

    @abstractmethod
    def insert_data(self, conn, cursor, input_file):
        """Insert data"""

    @abstractmethod
    def show_data(self, cursor, query):
        """Show data"""
