# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

**Project Overview:**
I developed a Streamlit application that seamlessly integrates with the YouTube API v3 to efficiently collect and manage data related to YouTube channels, videos, comments, and playlists. The integration with YouTube API v3 serves as the backbone for retrieving up-to-date and accurate information directly from the YouTube platform.

**YouTube API v3 Integration:**

**1. Data Retrieval:**

Leveraging the capabilities of the YouTube API v3, the application fetches detailed information about channels, videos, comments, and playlists. This ensures that the data collected is current, reflecting the latest content available on YouTube.

**2. Authentication:**

The application employs the necessary authentication mechanisms provided by the YouTube API v3 to ensure secure and authorized access to YouTube's data. This authentication is crucial for maintaining the integrity of the data retrieval process.

**Data Storage Workflow:**

**1. MongoDB Storage:**

Upon retrieval from the YouTube API, the data is initially stored in a MongoDB database. MongoDB's flexible document-based storage proves advantageous for handling diverse and evolving data structures associated with YouTube content.

**2. PostgreSQL Migration:**

To enhance data organization and support relational queries, I implemented a migration process to transfer the data from MongoDB to a PostgreSQL database. The PostgreSQL database is structured into four tables, corresponding to channels, videos, comments, and playlists.

**User Interface Features:**

**1. Channel ID Entry:**
Users can input a channel ID via a designated field in the app. This feature ensures streamlined data entry while displaying existing channel IDs to prevent duplicate entries and maintain data integrity.

**2. Upload Button (MongoDB):**

The "Upload" button facilitates the addition of new data to the MongoDB database. This ensures that the application remains up-to-date with the latest information from YouTube.

**3. Migrate Button (MongoDB to PostgreSQL):**

A "Migrate" button simplifies the process of transferring data from MongoDB to PostgreSQL. This feature supports a seamless transition between the two databases, allowing for improved data management.

**4. Radio Button Interface:**

Users can select from radio buttons to choose the type of data they want to view. The options include channels, videos, comments, and playlists, providing a straightforward way to access specific information.

**5. Pre-defined Queries:**

To simplify data retrieval from the PostgreSQL database, the application incorporates a list of pre-defined queries. Users can click on a specific query to obtain tailored information without needing to write complex SQL commands.

**Conclusion**

The Streamlit application is designed with a user-friendly interface, ensuring that users can easily navigate and interact with the features provided. The combination of the YouTube API v3 and MongoDB and PostgreSQL databases, along with thoughtful UI elements, offers a comprehensive platform for data storage, retrieval, and analysis. The application caters to both simplicity in data entry and flexibility in querying, making it a valuable tool for users handling YouTube content information.
