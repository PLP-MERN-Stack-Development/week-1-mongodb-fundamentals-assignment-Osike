const { MongoClient } = require('mongodb');

// Main function to run all queries
async function run() {
    const client = new MongoClient('mongodb://localhost:27017');
    try {
        await client.connect();
        const database = client.db('plp_bookstore');
        const books = database.collection('books');

        // Task 2: Basic CRUD Operations
        await runCrudOperations(books);
        
        // Task 3: Advanced Queries
        await runAdvancedQueries(books);
        
        // Task 4: Aggregation Pipelines
        await runAggregationPipelines(books);
        
        // Task 5: Indexing
        await runIndexing(books);

    } finally {
        await client.close();
    }
}

// Function to perform CRUD operations -task2
async function runCrudOperations(books) {
    console.log("=== CRUD Operations ===");

    // Insert Books (if needed)
    const bookDocuments = [
        { title: "To Kill a Mockingbird", author: "Harper Lee", genre: "Fiction", published_year: 1960, price: 12.99, in_stock: true, pages: 336, publisher: "J. B. Lippincott & Co." },
        { title: "1984", author: "George Orwell", genre: "Dystopian", published_year: 1949, price: 10.99, in_stock: true, pages: 328, publisher: "Secker & Warburg" },
        
    ];

    // Check if collection already has documents
    const count = await books.countDocuments();
    if (count === 0) {
        const insertResult = await books.insertMany(bookDocuments);
        console.log(`${insertResult.insertedCount} books were successfully inserted into the database`);
    }

    // 1. Find all books in a specific genre
    const genreBooks = await books.find({ genre: "Fiction" }).toArray();
    console.log("Fiction Books:", genreBooks);

    // 2. Find books published after a certain year
    const recentBooks = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log("Books Published After 1950:", recentBooks);

    // 3. Find books by a specific author
    const authorBooks = await books.find({ author: "George Orwell" }).toArray();
    console.log("Books by George Orwell:", authorBooks);

    // 4. Update the price of a specific book
    await books.updateOne(
        { title: "1984" },
        { $set: { price: 9.99 } }
    );
    console.log("Updated the price of '1984'.");

    // 5. Delete a book by its title
    await books.deleteOne({ title: "Moby Dick" });
    console.log("Deleted 'Moby Dick' from the collection.");
}

// Function to perform advanced queries -task3
async function runAdvancedQueries(books) {
    console.log("=== Advanced Queries ===");

    // Find books that are in stock and published after 2010
    const inStockRecentBooks = await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray();
    console.log("In Stock Books Published After 2010:", inStockRecentBooks);

    // Projection to return only title, author, and price fields
    const projectedBooks = await books.find({}, { projection: { title: 1, author: 1, price: 1 } }).toArray();
    console.log("Projected Books:", projectedBooks);

    // Sorting by price (ascending and descending)
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log("Books Sorted by Price Ascending:", sortedAsc);

    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log("Books Sorted by Price Descending:", sortedDesc);

    // Pagination (5 books per page)
    const page = 0; 
    const pageSize = 5;
    const paginatedBooks = await books.find().skip(page * pageSize).limit(pageSize).toArray();
    console.log("Paginated Books:", paginatedBooks);
}

// Function to run aggregation pipelines -task4
async function runAggregationPipelines(books) {
    console.log("=== Aggregation Pipelines ===");

    // Average price of books by genre
    const averagePriceByGenre = await books.aggregate([
        { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("Average Price by Genre:", averagePriceByGenre);

    // Find the author with the most books
    const mostBooksAuthor = await books.aggregate([
        { $group: { _id: "$author", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 }
    ]).toArray();
    console.log("Author with Most Books:", mostBooksAuthor);

    // Group books by publication decade and count them
    const booksByDecade = await books.aggregate([
        {
            $group: {
                _id: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] },
                count: { $sum: 1 }
            }
        }
    ]).toArray();
    console.log("Books by Publication Decade:", booksByDecade);
}

// Function to create indexes -task5
async function runIndexing(books) {
    console.log("=== Indexing ===");

    // Create an index on the title field
    await books.createIndex({ title: 1 });
    console.log("Index on title created.");

    // Create a compound index on author and published_year
    await books.createIndex({ author: 1, published_year: 1 });
    console.log("Compound index on author and published_year created.");

    // Use explain() to show performance improvement
    const explainResult = await books.find({ title: "1984" }).explain("executionStats");
    console.log("Explain Result:", explainResult);
}

// Run the main function
run().catch(console.error);