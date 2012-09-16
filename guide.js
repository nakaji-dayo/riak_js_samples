var async = require('async');
require('colors');

var db = require('riak-js').getClient({debug: true});

var logMeta = function(meta){
    console.log('statusCode:'+meta.statusCode+', contentType:'+meta.contentType);
};

//sample get
var getBeer = function(callback){
    console.log('get beer from drinks'.bold);
    db.get('drinks', 'beer', function(err, beer, meta) {
        if (err)
            throw err
        console.log(beer);
        logMeta(meta);
        callback();
    });
}
var getTea = function(callback){
    console.log('get tea from drinks'.bold);
    db.get('drinks', 'tea', function(err, drink, meta) {
        if (err)
            throw err
        console.log(drink);
        logMeta(meta);
        callback();
    });
}

//sample head(only get meta)
var head = function(callback){
    console.log('get only head'.bold);
    db.head('drinks', 'beer', function(err, data, meta) {
        if(err)
            throw err;
        console.log(data);
        logMeta(meta);
        callback();
    });
};

//sample is exists
var exists = function(callback){
    console.log('check exists beer'.bold);
    db.exists('drinks', 'beer', function(err, isExists, meta){
        if(err)
            throw err;
        console.log(isExists);
        logMeta(meta);
        callback();
    });
};

//sample get all
var getAll = function(callback){
    console.log('get all drinks'.bold);
    db.getAll('drinks', function(err, drinks, meta){
        if(err)
            throw err;
        console.log(drinks);
        logMeta(meta);
        callback();
    });
};

//sample get buckets
var buckets = function(callback){
    console.log('get buckets'.bold);
    db.buckets(function(err, buckets, meta){
        if(err)
            throw err;
        console.log(buckets);
        logMeta(meta);
        callback();
    });
};

//sample get keys
var keys = function(callback){
    console.log('get keys from drinks'.bold);
    db.keys('drinks', function(err, keys){
        if(err)
            throw err;
        console.log(keys);
        callback();
    });
};

//sample count
var count = function(callback){
    console.log('count drinks'.bold);
    db.count('drinks', function(err, count, meta){
        if(err)
            throw err;
        console.log('drinks:'+count);
        logMeta(meta);
        db.count('refrigerator', function(err, count, meta){
            if(err)
                throw err;
            console.log('refrigerator:'+count);
            logMeta(meta);
            callback();
        });
    });
};

//sample save
var saveTea = function(callback){
    console.log('save tea to drinks'.bold);
    db.save('drinks', 'tea', {description: 'Tea is an aromatic beverage commonly prepared by pouring boiling hot water over cured leaves of the Camellia sinensis plant.'}, function(err, data ,meta){
        if(err)
            throw err;
        console.log(data);
        logMeta(meta);
        callback();
    });
};
var saveBeer = function(callback){
    console.log('save beer to drinks'.bold);
    db.save('drinks', 'beer', {description: "Beer is the world's most widely consumed alcoholic beverage; it is the third-most popular drink overall, after water and tea."}, function(err, data ,meta){
        if(err)
            throw err;
        console.log(data);
        logMeta(meta);
        callback();
    });
};

//sample rm tea
var remove = function(callback){
    console.log('remove tea'.bold);
    db.remove('drinks', 'tea', function(err, data ,meta){
        if(err)
            throw err;
        console.log(data);
        logMeta(meta);
        callback();
    });
};
//sample remove all
var removeAll = function(callback){
    console.log('remove all'.bold);
    async.forEach(['drinks','refrigerator'],function(bucket,callback){
        db.keys(bucket, function(err, keys){
            async.forEach(keys, function(key,callback){
                db.remove(bucket,key,function(err, data, meta){
                    callback();
               });
             }, function(err){
                callback();
            });
        });
    },function(){
        callback();
    });
};


//sample stream keys
var stream = function(callback){
    console.log('stream keys'.bold);
    db.keys('drinks', {keys:'stream'}).
            on('keys', function(key){
                console.log(key);//during map-reduce?
            }).start();
    callback();
};

//sample save with link
var saveVedett = function(callback){
    console.log('save vedett to refrigerator, and link beer'.bold);
    db.save('refrigerator', 'vedett', {name: " VEDETT EXTRA WHITE"}, 
            {links:[{bucket:'drinks', key:'beer', tag:'category'}]},
            function(err, data ,meta){
                if(err)
                    throw err;
                console.log(data);
                logMeta(meta);
                callback();
            });
};

//sample walk
var walk = function(callback){
    console.log('walk category of vedett'.bold);
    db.walk('refrigerator', 'vedett', [["_","category"]], function(err, data ,meta){
        if(err)
            throw err;
        console.log(data);
        logMeta(meta);
        callback();
    });
};

async.series([
    buckets,
    exists,
    saveTea,
    getTea,
    count,
    remove,
    saveTea,
    saveBeer,
    keys,
    exists,
    getAll,
    saveVedett,
    walk,
    removeAll,
    count,
]);

//head();
//get();

