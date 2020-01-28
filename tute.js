/*let students = [
    {name: "Dale Cooper", class: "Calculus", tests: [30, 28, 45]},
    {name: "Harry Truman", class: "Geometry", tests: [28, 26, 44]},
    {name: "Shelly Johnson", class: "Calculus", tests: [27, 26, 43]},
    {name: "Bobby Briggs", class: "College Algebra", tests: [20, 18, 35]},
    {name: "Donna Heyward", class: "Geometry", tests: [28, 28, 44]},
    {name: "Audrey Horne", class: "College Algebra", tests: [22, 26, 44]},
    {name: "James Hurley", class: "Calculus", tests: [20, 20, 38]},
    {name: "Lucy Moran", class: "College Algebra", tests: [26, 24, 40]},
    {name: "Tommy Hill", class: "College Algebra", tests: [30, 29, 46]},
    {name: "Andy Brennan", class: "Geometry", tests: [20, 21, 38]}
];

// non-map way of printing name info from the above structure
let studNames = [];
for (let i = 0; i < students.length; i++) {
    studNames.push(students[i].name);
}
console.log(studNames);

// map way of doing it by cycling through array of student objects
let studInfo = students.map((x) => {
    return x.name + 'is in ' + x.class + ' and has scores: ' + x.tests;
});

console.log(studInfo);

// reduce way of summing scores -- takes an accumulator and currentValue as callback params to execute on each elem
let tests = [{score: 30}, {score: 28}, {score: 45}];
let testSum = tests.reduce((currSum, tests) => {
    return currSum + tests.score;
}, 0);
console.log(testSum); */

//------------------ start mongodb node.js client
let MongoClient = require('mongodb').MongoClient;
let url = "mongodb://localhost:27017/";

let classData = [{
    class: "Philosophy 101",
    startDate: new Date(2016, 1, 10),
    students: [
        {fName: "Dale", lName: "Cooper", age: 42},
        {fName: "Lucy", lName: "Moran", age: 35},
        {fName: "Tommy", lName: "Hill", age: 44}
    ],
    cost: 1600,
    professor: "Paul Slugman",
    topics: "Socrates,Plato,Aristotle,Francis Bacon",
    book:
        {
            isbn: "1133612105",
            title: "Philosophy : A Text With Readings",
            price: 165.42
        }
}, {
    class: "College Algebra",
    startDate: new Date(2016, 1, 11),
    students: [
        {fName: "Dale", lName: "Cooper", age: 42},
        {fName: "Laura", lName: "Palmer", age: 22},
        {fName: "Donna", lName: "Hayward", age: 21},
        {fName: "Shelly", lName: "Johnson", age: 24}
    ],
    cost: 1500,
    professor: "Rhonda Smith",
    topics: "Rational Expressions,Linear Equations,Quadratic Equations",
    book:
        {
            isbn: "0321671791",
            title: "College Algebra",
            price: 179.40
        }
}, {
    class: "Astronomy 101",
    startDate: new Date(2016, 1, 11),
    students: [
        {fName: "Bobby", lName: "Briggs", age: 21},
        {fName: "Laura", lName: "Palmer", age: 22},
        {fName: "Audrey", lName: "Horne", age: 20}
    ],
    cost: 1650,
    professor: "Paul Slugman",
    topics: "Sun,Mercury,Venus,Earth,Moon,Mars",
    book:
        {
            isbn: "0321815351",
            title: "Astronomy: Beginning Guide to Univ",
            price: 129.45
        }
}, {
    class: "Geology 101",
    startDate: new Date(2016, 1, 12),
    students: [
        {fName: "Andy", lName: "Brennan", age: 36},
        {fName: "Laura", lName: "Palmer", age: 22},
        {fName: "Audrey", lName: "Horne", age: 20}
    ],
    cost: 1450,
    professor: "Alice Jones",
    topics: "Earth,Moon,Elements,Minerals",
    book:
        {
            isbn: "0321814061",
            title: "Earth : An Introduction to Physical Geology",
            price: 130.65
        }
}, {
    class: "Biology 101",
    startDate: new Date(2016, 1, 11),
    students: [
        {fName: "Andy", lName: "Brennan", age: 36},
        {fName: "James", lName: "Hurley", age: 25},
        {fName: "Harry", lName: "Truman", age: 41}
    ],
    cost: 1550,
    professor: "Alice Jones",
    topics: "Earth,Cell,Energy,Genetics,DNA",
    book:
        {
            isbn: "0547219474",
            title: "Holt McDougal Biology",
            price: 104.30
        }
}, {
    class: "Chemistry 101",
    startDate: new Date(2016, 1, 13),
    students: [
        {fName: "Bobby", lName: "Briggs", age: 21},
        {fName: "Donna", lName: "Hayward", age: 21},
        {fName: "Audrey", lName: "Horne", age: 20},
        {fName: "James", lName: "Hurley", age: 25}
    ],
    cost: 1600,
    professor: "Alice Jones",
    topics: "Matter,Energy,Atom,Periodic Table",
    book:
        {
            isbn: "0547219474",
            title: "Chemistry : Matter and Change",
            price: 104.30
        }
}];

// MongoClient.connect(url, (err, db) => {
//     if (err) throw err;
//     let dbo = db.db('scratchdb');
//     dbo.createCollection('classes', (err, res) => {
//         if (err) throw err;
//         console.log('Collection created');
//     });
//     dbo.collection("classes").insertMany(classData, (err, res) => {
//         if (err) throw err;
//         console.log('Num docs inserted: ' + res.insertedCount);
//         db.close();
//     });
// });

// N.B. arrow functions cannot access "this"
let mapFunc = function () {
    /**
     * Map called for every document in the collection. Send total student name list to reduce
     */
    for (let i = 0; i < this.students.length; i++) {
        let student = this.students[i];
        emit(student.fName + " " + student.lName, 69420); // emit value of 69420 to reiterate that reduce() only executes for keys with multiple values
    }
};

let reduceFunc = function (student, values) {
    /**
     * N.B.: MONGO ONLY APPLIES REDUCE() FOR KEYS WITH MULTIPLE VALUES
     * Called by map. Receives all the values for a given key. Then we sum the total num time the student's name shows
     * in the collection of documents.
     *
     * How it works: Essentially, map() will spit all the values for a given key (i.e. total student appears).
     * This reduce() will take in that K:V and count the total times and output an aggregated value for each key,
     * which is the total count.
     * and sum
     * @type {number}
     */
    let count = values.length; // can just use length of values array to ascertain counts of students
    // for (let i = 0; i < values.length; i++) {
    //     count += i;
    // }
    return count;  // return aggregated value for the given student
};

MongoClient.connect(url, (err, db) => {
    if (err) throw err;
    let dbo = db.db('scratchdb');
    /*dbo.createCollection('classes', (err, res) => {
        if (err) throw err;
        console.log('Collection created');
    });
    dbo.collection("classes").insertMany(classData, (err, res) => {
        if (err) throw err;
        console.log('Num docs inserted: ' + res.insertedCount);
        db.close();
    });*/
    dbo.collection("classes").mapReduce(mapFunc, reduceFunc, {out: "map_ex1"}, (err, res) => {
        if (err) throw err;
        db.close();
    })
});


// New TASK: Tally the total number of classes each professor teaches
let mapFunc2 = function () {
    emit(this.professor, 1); // this one's easy: simply emit the professor and a random value
};

let reduceFunc2 = function (profName, values) {
    return values.length; // also easy: simply return the length of the values list, indicating all professors in collection
};

MongoClient.connect(url, (err, db) => {
    if (err) throw err;
    let dbo = db.db('scratchdb');
    dbo.collection("classes").mapReduce(mapFunc2, reduceFunc2, {out: "map_ex2"}, (err, res) => {
        if (err) throw err;
        db.close();
    })
});


// New TASK: Tally the number of times each topic is covered in all the subjects (documents) in the collection
let mapFunc3 = function () {
    // the this.topics attribute is comma-separated --> split this
    topics = this.topics.split(',');
    // emit each topic with a random value
    for (let i = 0; i < topics.length; i++) {
        emit(topics[i], 1);
    }
};

let reduceFunc3 = function (topic, values) {
    return values.length; // same deal since we are still on the task of counting up stuff
};

MongoClient.connect(url, (err, db) => {
    if (err) throw err;
    let dbo = db.db('scratchdb');
    dbo.collection("classes").mapReduce(mapFunc3, reduceFunc3, {out: "map_ex3"}, (err, res) => {
        if (err) throw err;
        db.close();
    })
});


// New TASK: Sum the costs for each class a professor teaches as well as count of each => finalize to average cost
let mapFunc4 = function () {
    emit(this.professor, {count: 1, cost: this.cost}); // emit professor:{count: count, cost:cost}
};

let reduceFunc4 = function (profName, values) {
    let value = {count: 0, cost: 0}; // first aggregate the count and cost for each professor in a similar format
    for (let i = 0; i < values.length; i++) {
        value["count"] += values[i].count; // tally the count for this professor
        value["cost"] += values[i].cost; // tally the total cost of this professor's classes
    }
    return value;
};

let finalizeFunc4 = function (profName, reducedValue) {
    /*
    Input is a K:V pair, professor name and a reduced value. We introduce a new variable average calculated
    by dividing total cost by the total count and output this new reduced value.
     */
    reducedValue.average = reducedValue.cost / reducedValue.count;
    return reducedValue;
};

MongoClient.connect(url, (err, db) => {
    if (err) throw err;
    let dbo = db.db('scratchdb');
    dbo.collection("classes").mapReduce(mapFunc4, reduceFunc4, {
        out: "map_ex4",
        finalize: finalizeFunc4
    }, (err, res) => {
        if (err) throw err;
        db.close();
    })
});