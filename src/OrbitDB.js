'use strict'

const path = require('path')
const EventStore = require('orbit-db-eventstore')
const FeedStore = require('orbit-db-feedstore')
const KeyValueStore = require('orbit-db-kvstore')
const CounterStore = require('orbit-db-counterstore')
const DocumentStore = require('orbit-db-docstore')
const Pubsub = require('orbit-db-pubsub')
const Cache = require('orbit-db-cache')
const Keystore = require('orbit-db-keystore')
const AccessController = require('./ipfs-access-controller')
const OrbitDBAddress = require('./orbit-db-address')

const Logger = require('logplease')
const logger = Logger.create("orbit-db")
Logger.setLogLevel('NONE')

const validTypes = ['eventlog', 'feed', 'docstore', 'counter', 'keyvalue']

class OrbitDB {
  constructor(ipfs, directory, options = {}) {
    this._ipfs = ipfs
    this.id = options.peerId || (this._ipfs._peerInfo ? this._ipfs._peerInfo.id._idB58String : 'default')
    this._pubsub = options && options.broker 
      ? new options.broker(this._ipfs) 
      : new Pubsub(this._ipfs, this.id)
    this.stores = {}
    this.types = validTypes
    this.directory = directory || './orbitdb'
    this.keystore = new Keystore(path.join(this.directory, this.id, '/keystore'))
    this.key = this.keystore.getKey(this.id) || this.keystore.createKey(this.id)
  }

  /* Databases */
  async feed(address, options = {}) {
    options = Object.assign({ create: true, type: 'feed' }, options)
    return this.open(address, options)
  }

  async eventlog(address, options = {}) {
    options = Object.assign({ create: true, type: 'eventlog' }, options)
    return this.open(address, options)
  }

  async keyvalue (address, options) {
    options = Object.assign({ create: true, type: 'keyvalue' }, options)
    return this.open(address, options)
  }

  async kvstore(address, options) {
    return this.keyvalue(address, options)
  }

  async counter(address, options = {}) {
    options = Object.assign({ create: true, type: 'counter' }, options)
    return this.open(address, options)
  }

  async docstore(address, options = {}) {
    options = Object.assign({ create: true, type: 'docstore' }, options)
    return this.open(address, options)
  }

  disconnect() {
    Object.keys(this.stores).forEach((e) => this.stores[e].close())
    if (this._pubsub) this._pubsub.disconnect()
    this.stores = {}
  }

  /*
    options = {
      write: [] // array of keys that can write to this database
    }
  */
  async create (name, type, options = {}) {
    if (!OrbitDB.isValidType(type))
      throw new Error(`Invalid database type '${type}'`)

    // The directory to look databases from can be passed in as an option
    const directory = options.directory || this.directory
    logger.debug(`Creating database '${name}' as ${type} in '${directory}'`)

    if (OrbitDBAddress.isValid(name))
      throw new Error(`Given database name is an address. Please give only the name of the database!`)

    // Create an AccessController
    const accessController = new AccessController(this._ipfs)
    // Add ourselves as the admin of the database
    accessController.add('admin', this.key.getPublic('hex'))
    // Add keys that can write to the database
    if (options && options.write) {
      options.write.forEach(e => accessController.add('write', e))
    }
    // Save the Access Controller in IPFS
    const accessControllerAddress = await accessController.save()

    // Creates a DB manifest file
    const createDBManifest = () => {
      return {
        name: name,
        type: type,
        accessController: path.join('/ipfs', accessControllerAddress),
      }
    }

    // Save the manifest to IPFS
    const manifest = createDBManifest()
    const dag = await this._ipfs.object.put(new Buffer(JSON.stringify(manifest)))
    const manifestHash = dag.toJSON().multihash.toString()

    // Create the database address
    const address = path.join('/orbitdb', manifestHash, name)
    const dbAddress = OrbitDBAddress.parse(address)

    // Load local cache
    this._cache = new Cache(path.join(directory), path.join(dbAddress.root, dbAddress.path))
    await this._cache.load()

    // Check if we already have the database
    const haveDB = await this._cache.get('manifest')

    if (haveDB && !options.overwrite)
      throw new Error(`Database '${dbAddress}' already exists!`)

    // Save the database locally
    await this._cache.set('manifest', dbAddress.root)

    logger.debug(`Created database '${dbAddress}'`)

    // Open the database
    return this.open(dbAddress, options)
  }

  /*
      options = {
        localOnly: false // if set to true, throws an error if database can't be found locally
        create: false // wether to create the database
      }
   */
  async open (address, options = {}) {
    options = Object.assign({ localOnly: false, create: false }, options)
    logger.debug(`Open database '${address}'`)

    // The directory to look databases from can be passed in as an option
    const directory = options.directory || this.directory
    logger.debug(`Look from '${directory}'`)

    // If address is just the name of database, check the options to crate the database
    if (!OrbitDBAddress.isValid(address)) {
      if (!options.create) {
        throw new Error(`'options.create' set to 'false'. If you want to create a database, set 'options.create' to 'true'.`)
      } else if (options.create && !options.type) {
        throw new Error(`Database type not provided! Provide a type with 'options.type' (${validTypes.join('|')})`)
      } else {
        logger.warn(`Not a valid OrbitDB address '${address}', creating the database`)
        options.overwrite = options.overwrite ? options.overwrite : true
        return this.create(address, options.type, options)        
      }
    }

    // Parse the database address
    const dbAddress = OrbitDBAddress.parse(address)

    // Load local cache
    this._cache = new Cache(path.join(directory), path.join(dbAddress.root, dbAddress.path))
    await this._cache.load()

    // Check if we have the database
    const haveDB = await this._cache.get('manifest')
    logger.debug((haveDB ? 'Found' : 'Didn\'t find') + ` database '${dbAddress}'`)

    // If we want to try and open the database local-only, throw an error
    // if we don't have the database locally
    if (options.localOnly && !haveDB) {
      logger.error(`Database '${dbAddress}' doesn't exist!`)
      throw new Error(`Database '${dbAddress}' doesn't exist!`)
    }

    logger.debug(`Loading Manifest for '${dbAddress}'`)

    // Get the database manifest from IPFS
    const dag = await this._ipfs.object.get(dbAddress.root)
    const manifest = JSON.parse(dag.toJSON().data)
    logger.debug(`Manifest for '${dbAddress}':\n${JSON.stringify(manifest, null, 2)}`)

    // Make sure the type from the manifest matches the type that was given as an option
    if (options.type && manifest.type !== options.type)
      throw new Error(`Database '${dbAddress}' is type '${manifest.type}' but was opened as '${options.type}'`)

    // Save the database locally
    await this._cache.set('manifest', dbAddress.root)
    logger.debug(`Saved manifest to IPFS as '${dbAddress.root}'`)

    // Open the the database
    options = Object.assign({}, options, { accessControllerAddress: manifest.accessController })
    return this._openDatabase(dbAddress, manifest.type, options)
  }

  async _openDatabase (address, type, options) {
    if (type === 'counter')
      return this._createStore(CounterStore, address, options)
    else if (type === 'eventlog')
      return this._createStore(EventStore, address, options)
    else if (type === 'feed')
      return this._createStore(FeedStore, address, options)
    else if (type === 'docstore')
      return this._createStore(DocumentStore, address, options)
    else if (type === 'keyvalue')
      return this._createStore(KeyValueStore, address, options)
    else
      throw new Error(`Invalid database type '${type}'`)
  }

  /* Private methods */
  async _createStore(Store, address, options) {
    const addr = address.toString()

    let accessController
    if (options.accessControllerAddress) {
      accessController = new AccessController(this._ipfs)
      await accessController.load(options.accessControllerAddress)
    }

    const opts = Object.assign({ replicate: true }, options, { 
      accessController: accessController, 
      keystore: this.keystore,
      cache: this._cache,
    })

    const store = new Store(this._ipfs, this.id, address, opts)
    store.events.on('write', this._onWrite.bind(this))
    store.events.on('ready', this._onReady.bind(this))
    store.events.on('close', this._onClose.bind(this))

    this.stores[addr] = store

    if(opts.replicate && this._pubsub)
      this._pubsub.subscribe(addr, this._onMessage.bind(this))

    return store
  }

  // Callback for receiving a message from the network
  _onMessage(address, heads) {
    const store = this.stores[address]
    try {
      store.sync(heads)
    } catch (e) {
      logger.error(e)
    }
  }

  // Callback for local writes to the database. We the update to pubsub.
  _onWrite(address, hash, entry, heads) {
    if(!heads) throw new Error("'heads' not defined")
    if(this._pubsub) setImmediate(() => this._pubsub.publish(address, heads))
  }

  // Callback for database being ready
  _onReady(address, heads) {
    if(heads && this._pubsub) {
      setTimeout(() => this._pubsub.publish(address, heads), 1000)
    }
  }

  _onClose(address) {
    if(this._pubsub) 
      this._pubsub.unsubscribe(address)

    const store = this.stores[address]

    if (store) {
      store.events.removeAllListeners('write')
      store.events.removeAllListeners('ready')
      store.events.removeAllListeners('close')
      store.close()
      delete this.stores[address]
    }
  }

  static isValidType (type) {
    return validTypes.includes(type)
  }
}

module.exports = OrbitDB
