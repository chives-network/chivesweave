# ChivesDB Decentralized Database
ChivesDB is a decentralized database product that utilizes RocksDb for data storage and provides query services through an Http API. It enables the creation, modification, and deletion of data by simply sending relevant transactions. The data storage and computation directly leverage the high-performance computers of miners, inherently possessing decentralized characteristics. ChivesDB offers its services on the Arweave chain.

## Scenarios for Using Decentralized Databases
1. Various application systems: Blogs, forums, websites, emails, etc.
2. Information management systems
3. Other blockchain applications

## Why a Decentralized Database is Needed
1. Only a database can support numerous applications. Whether it's a relational database or NoSQL, both allow data on the AR network to be presented in a more flexible way to different users. For example, developing a blog product requires storing user data, articles, likes, and reading counts in a database, which is also essential for forums and websites.
2. Limitations of current AR gateway products. The existing gateway products in AR use a PG database for data storage, functioning more like a blockchain explorer with features for querying blockchain and transactions. However, they lack extensive involvement in processing AR chain data. Additionally, the need for extra deployment and the difficulty in deployment make it challenging to fulfill decentralization requirements. The scarcity of such nodes impedes meeting the demands for decentralization.
3. Summary: The widespread demand and the lack of related products make the need for an efficient decentralized database product in this field urgent.

## How to Implement a Decentralized Database
1. Implementing Database Functions
   1) Develop directly on AR nodes, which already integrate RocksDb, an efficient NoSQL product used by AR nodes to store block and transaction information. RocksDb features can be used to fulfill NoSQL functions.
   2) Filter block data directly in mining nodes, obtain all transaction records, update and generate RocksDb, and then provide services externally through an Http API.
   3) RocksDb has transaction capabilities, suitable for handling special cases like blockchain block rollbacks.
   4) Utilize some features of RocksDb to implement a simple relational database model, such as a two-dimensional data table with only one primary key.
   5) Use an additional field to mark previous data records with the block height, hash the content using SHA256, enabling data verification among different nodes or rolling back data on the current node, similar to blocks in a blockchain.

2. Implementing Decentralization
   1) Inherent decentralization is achieved by integrating this functionality directly into the mining node software, with as many nodes as there are miners providing services.
   2) Mining computers used for mining have high performance, and the RandomX algorithm relies on a single CPU core for calculations, benefiting from the typically multi-core CPUs in mining computers. This ensures efficient utilization of CPU resources for RocksDb databases.
   3) Mining computers have large hard drive space, although the entire AR requires around 140TB of hard drive space. However, for serving a decentralized database, usually only 100-300GB of hard drive space is sufficient, avoiding significant additional space consumption.
   4) Using high-performance mining computers directly as a solution for decentralized databases has inherent decentralization characteristics and does not significantly increase additional hardware costs.

## Significance of Portals to AR
1. Portal Concept
   Portals are collections of data and resources presented to different users through algorithms on platforms with varying content. Think of platforms like YouTube's homepage or TikTok's recommendation algorithm.

2. Why Portals Are Necessary
   Without the concept of portals, AR would exist only as a provider of underlying services like data storage, while online, traffic is paramount. To extend AR upwards, data stored on AR needs to be displayed in various ways. Only with convenient and quick data presentation, tailored to different users, can AR move beyond the perception of being just a data storage service, becoming a comprehensive platform similar to YouTube.

3. Can Portals Be Subdivided
   Yes, they should be subdivided. Different portal platforms can be introduced for various domains, such as platforms dedicated to SOL, ETH-related data or categorized by types like NFTs, games, blogs, videos, audios, images, etc. These portals, generated based on different domains and types, can serve users well. This does not involve complex recommendation algorithms, a topic that can be discussed separately.

4. Relationship Between Portals and Applications
   Portals aggregate various information, while applications are developed by third-party developers and act as data creators. Although applications can also have their own portals, here we are referring to portals for the entire AR, a concept with some differences. Applications use AR to store data, and portals categorically display data. For more detailed data presentation or understanding the logical relationships between different data, one needs to go into a specific application. Portals provide an additional traffic entry point for applications, somewhat analogous to the relationship between the official YouTube platform and various YouTubers.

5. Similar Products in the Market
   I have analyzed several related products, most of which focus on data statistics and analysis. If that is the case, [viewblock.io/arweave](https://viewblock.io/arweave) is the best in this regard. Other data analysis products are still within the scope of data statistics and analysis. Personally, I believe the reasons why similar products haven't emerged are mainly concentrated in: 1) The vast amount of data on the AR chain makes technical implementation challenging, 2) Limited thinking, not considering such content in the past, 3) Temporary inability to see the economic benefits of such products.

6. How to Implement Portal Functions
   Implementing such a product requires high-configured servers and a supporting database (not necessarily decentralized). I will elaborate on how to implement such a product in other chapters.