# Data Aggregation Portal Platform based on ARWEAVE Network

## Main Features
The Data Aggregation Portal Platform, abbreviated as "Portal," based on the ARWEAVE network can extract documents, images, videos, and text resources from the entire AR network. It forms a system that allows for searching, categorizing, and on-demand display, making the resources on the entire AR network transparent and efficient.

## Significance of Portal for AR
1. **Concept of Portal:**
   A portal is a collection of data and resources presented to different users on a platform using certain algorithms. You can think of it as similar to the homepage of YouTube or the recommendation algorithm of TikTok.
2. **Why Portals are Needed:**
   Without the concept of portals, AR would only serve as a provider of low-level services for data storage. On the internet, traffic is crucial. To extend AR upwards, data stored on AR needs to be displayed in various ways. Only through convenient and quick data presentation, as well as showing different content to different users, can AR move away from the perception of being just a data storage service. It can then become an all-encompassing entry platform similar to YouTube.
3. **Can Portals be Subdivided:**
   Yes, they can and should be subdivided. Different portal platforms can be introduced for different domains. For example, data related to SOL and ETH can have a dedicated portal, or portals can be categorized by type such as NFT, gaming, blogs, videos, audio, images, etc. These portals, generated based on different domains and types, can effectively serve users. Of course, this does not involve complex recommendation algorithms, which can be discussed separately.
4. **Relationship Between Portals and Applications:**
   Portals aggregate various information, while applications are developed by third-party developers and act as producers of various data. Applications can have their own portals, but here we are discussing portals oriented towards the entire AR, and the concepts are somewhat different. Applications use AR to store data, and portals categorize and display data. If more detailed data display is needed or to understand the logical relationships between different data, it is necessary to go into a specific application. Portals provide an additional traffic entry point for applications and, to some extent, can be likened to the relationship between YouTube's official platform and various YouTubers.
5. **Are There Similar Products in the Market:**
   I have analyzed several related products, most of which focus on data statistics and analysis. If that's the case, [viewblock.io/arweave](https://viewblock.io/arweave) is the best one. Other data analysis products are still within the realm of data statistics and analysis. I personally think the reasons why portal products have not appeared are probably concentrated in: 1) Too much data on the AR chain, making the technical implementation too challenging; 2) Limited thinking, not previously considering this aspect of content; 3) Temporarily not seeing the economic benefits of such products.
6. **How to Implement Portal Functions:**
   It requires high-configured servers and accompanying databases (not necessarily decentralized) to support this product. I will explain in detail how to implement such a product in other sections.

# Technical Architecture
1. **Database:** SQLITE3 or MYSQL, ROCKSDB
2. **Programming Languages:** Erlang for the backend API, React for the frontend code
3. **Runtime Environment:** Running on mining nodes
4. **Decentralization:** Because it is deployed on mining nodes, it inherently has decentralized characteristics

# Key Features
1. **Upload Data:** Upload various files.
2. **On-chain Portal:** Parse on-chain data, classify, and search by keywords.
3. **Personal Profiles:** Classify and summarize records of a specific address to create a personal profile page. It also supports data searches under that user.
4. **Data Display:**
   1. Documents: Support online reading of various documents, generate document thumbnails, calculate page numbers, document hash, etc.
   2. Images: Support opening images online, generate image thumbnails to improve loading speed.
   3. Videos: Support online video playback or only enable download functionality. Miners can choose whether to enable online video playback based on traffic limitations.
   4. Other Formats: Strive to support more document types for online reading.

# Involved Technologies
1. Erlang
2. React
3. Arweave Wallet
4. Bundle

# Functional Analogy
1. Like a YouTube platform that can showcase various resources, but videos are not the main focus of this project.
2. Like a more powerful cloud storage, but resources are publicly accessible and searchable.
3. It is a blockchain data management system that can manage and retrieve various resources, reviving dormant data.

# Project Expansion (Not Implemented)
1. Decentralized Database
2. Decentralized Blog
3. Decentralized Website
4. Information Management System

# Project Showcase
The project showcase utilizes the Chivesweave chain, based on Arweave 2.7. The reason for the fork is that the project is currently in the authentication stage, requiring the upload of a large amount of data for testing and the development of some customized features. To meet economic and flexibility requirements, a fork was made from the main network.

- Cloud Storage: [Chivesweave Drive](https://drive.chivesweave.org/)
- GitHub: [Chives Network GitHub](https://github.com/chives-network)

Most of the provided features are already implemented on [Chivesweave Drive](https://drive.chivesweave.org/), and subsequent development work will focus on refining and completing the expansion aspects of the project.

# Team Members
Two developers are based in Canada, and one technical support member is located in South Korea.

# Investment and Financing
Currently, the system is considered crucial for AR as it can revive dormant data on the entire chain, increase user willingness to share, and shift user focus to AR. In the future, it may break free from the limitations of being just another chain storage layer and bring about a new prosperous ecosystem on the blockchain.

Due to the immense data on the AR chain, additional server resources are needed. The current project members are unable to bear the cost, so seeking some funding assistance within the AR ecosystem is being considered.
