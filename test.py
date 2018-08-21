import dq2 as dq


mydq = dq.dq()
yml = mydq.loadYmlFile('test.yml')
mydq.extractDataset('titanic')
res = mydq.executeRule('a1', True)
mydq.commit('a1')
