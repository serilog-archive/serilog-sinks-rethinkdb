
// All events that have an `exception`
r.db('logging').table('log').filter(function(log) {
    return log.hasFields('exception');
});


// All events that are have a level of 5
r.db('logging').table('log').filter({ level: 5 });

