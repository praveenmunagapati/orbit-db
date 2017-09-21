'use strict'

const assert = require('assert')
const OrbitDBAddress = require('../src/orbit-db-address')

// Obsolete tests, need to re-write
describe.skip('Address', function () {
  this.timeout(1000)

  const id = 'QmRFmum3AotnkdqFnKLYh8pAUxwnLi83EmgFH15LvQ2toA'
  const id2 = 'QmRFmum3AotnkdqFnKLYh8pAUxwnLi83EmgFH15LvQ2toB'

  const tests = [
    {
      description: 'single name',
      input: 'hello',
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'one sub-directory',
      input: 'hello/one',
      output: `/orbitdb/${id}/hello/one`,
    },
    {
      description: 'two sub-directories',
      input: 'hello/one/two',
      output: `/orbitdb/${id}/hello/one/two`,
    },
    {
      description: 'two three-directories',
      input:'hello/one/two/three',
      output: `/orbitdb/${id}/hello/one/two/three`,
    },
    {
      description: 'root path',
      input: '/hello',
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'root path and sub-directories',
      input: '/hello/one/two/three',
      output: `/orbitdb/${id}/hello/one/two/three`,
    },
    {
      description: 'with nodeId',
      input: `${id}/hello`,
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'with another node\'s nodeId',
      input: `${id2}/hello`,
      output: `/orbitdb/${id2}/hello`,
    },
    {
      description: 'with nodeId as root path',
      input: `/${id}/hello`,
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'with another node\'s nodeId as root path',
      input: `/${id2}/hello`,
      output: `/orbitdb/${id2}/hello`,
    },
    {
      description: 'simple name with protocol prefix',
      input: '/orbitdb/hello',
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'with protocol prefix',
      input: '/orbitdb/QmRFmum3AotnkdqFnKLYh8pAUxwnLi83EmgFH15LvQ2toA/hello',
      output: `/orbitdb/${id}/hello`,
    },
    {
      description: 'exclude databases called \'orbitdb\'',
      input: 'orbitdb/QmRFmum3AotnkdqFnKLYh8pAUxwnLi83EmgFH15LvQ2toA/hello',
      output: `/orbitdb/${id}/orbitdb/QmRFmum3AotnkdqFnKLYh8pAUxwnLi83EmgFH15LvQ2toA/hello`,
    },
    {
      description: 'can contain \'orbitdb\' elsewhere in the path',
      input: '/abdc/orbitdb/hello',
      output: `/orbitdb/${id}/abdc/orbitdb/hello`,
    },
    {
      description: 'takes an integer as an input',
      input: 777,
      output: `/orbitdb/${id}/777`,
    },
    {
      description: 'takes a float as an input',
      input: 7.7,
      output: `/orbitdb/${id}/7.7`,
    },
  ]

  const toOrbitDBAddress = (address) => OrbitDBAddress.parse(address)
  const verify = (address, expected) => assert.equal(address, expected)

  /* Test all input/output pairs */
  tests.forEach(test => {
    it(test.description, () => {
      const address = toOrbitDBAddress(test.input)
      console.log(test.input, "-->", address.toString())
      assert.equal(address.toString(), test.output)
    })
  })

  /* Test errors */
  it('throws an error if input is null or undefined', () => {
    let err
    try {
      const address = OrbitDBAddress.parse(null)
    } catch (e) {
      err = e
    }
    assert.equal(err.toString(), `Error: Not a valid orbit-db address: ${null}`)
  })
})
