import sqlite3 as sqlite
from math import tanh


def dtanh(y):
    """
    The sigmoid function is mathematically convenient 
    because we can represent its derivative in terms 
    of the output (y) of the function.
    tanh'(x) = 1 /(tanh(x)^2)
    """
    return 1.0 - (y * y)


class SearchNet:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def make_tables(self):
        self.con.execute('DROP TABLE IF EXISTS hiddennode')
        self.con.execute('DROP TABLE IF EXISTS wordhidden')
        self.con.execute('DROP TABLE IF EXISTS hiddenurl')

        self.con.execute('CREATE TABLE hiddennode(create_key)')
        self.con.execute('CREATE TABLE wordhidden(fromid,toid,strength)')
        self.con.execute('CREATE TABLE hiddenurl(fromid,toid,strength)')
        self.con.commit()

    def get_strength(self, fromid, toid, layer):
        if layer == 0:
            table = 'wordhidden'
        else:
            table = 'hiddenurl'

        res = self.con.execute('select strength from %s where fromid=%d and toid=%d' % (
            table, fromid, toid)).fetchone()

        if res == None:
            if layer == 0:
                return -0.2

            if layer == 1:
                return 0

        return res[0]

    def set_strength(self, fromid, toid, layer, strength):
        if layer == 0:
            table = 'wordhidden'
        else:
            table = 'hiddenurl'

        res = self.con.execute('select rowid from %s where fromid=%d and toid=%d' %
                               (table, fromid, toid)).fetchone()
        if res == None:
            self.con.execute('insert into %s (fromid,toid,strength) values (%d,%d,%f)' %
                             (table, fromid, toid, strength))
        else:
            rowid = res[0]
            self.con.execute('update %s set strength=%f where rowid=%d' %
                             (table, strength, rowid))

    def generate_hidden_node(self, word_ids, urls):
        if len(word_ids) > 3:
            return None

        # Check if we already created a node for this set of words
        createkey = '_'.join(sorted([str(wi) for wi in word_ids]))
        res = self.con.execute(
            "select rowid from hiddennode where create_key='%s'" % createkey).fetchone()

        # If not, create it
        if res == None:
            cur = self.con.execute(
                "insert into hiddennode (create_key) values ('%s')" % createkey)
            hiddenid = cur.lastrowid
            # Put in some default weights
            for wordid in word_ids:
                self.set_strength(wordid, hiddenid, 0, 1.0 / len(word_ids))
            for urlid in urls:
                self.set_strength(hiddenid, urlid, 1, 0.1)
            self.con.commit()

    def get_all_hidden_ids(self, word_ids, url_ids):
        l1 = {}
        for wid in word_ids:
            cur = self.con.execute(
                'select toid from wordhidden where fromid=%d' % wid)
            for row in cur:
                l1[row[0]] = 1

        for uid in url_ids:
            cur = self.con.execute(
                'select fromid from hiddenurl where toid=%d' % uid)
        for row in cur:
            l1[row[0]] = 1

        return list(l1.keys())

    def setup_network(self, word_ids, url_ids):
        # value lists
        self.word_ids = word_ids
        self.hidden_ids = self.get_all_hidden_ids(word_ids, url_ids)
        self.url_ids = url_ids

        # node outputs
        self.ai = [1.0] * len(self.word_ids)
        self.ah = [1.0] * len(self.hidden_ids)
        self.ao = [1.0] * len(self.url_ids)

        # create weights matrix
        self.wi = [[self.get_strength(wordid, hiddenid, 0)
                    for hiddenid in self.hidden_ids] for wordid in self.word_ids]
        self.wo = [[self.get_strength(hiddenid, urlid, 1)
                    for urlid in self.url_ids] for hiddenid in self.hidden_ids]

    def feed_forward(self):
        # the only inputs are the query words
        for w in range(len(self.word_ids)):
            self.ai[w] = 1.0

        # hidden activations
        for h in range(len(self.hidden_ids)):
            sum = 0.0
            for w in range(len(self.word_ids)):
                sum = sum + self.ai[w] * self.wi[w][h]
            self.ah[h] = tanh(sum)

        # output activations
        for u in range(len(self.url_ids)):
            sum = 0.0
            for h in range(len(self.hidden_ids)):
                sum = sum + self.ah[h] * self.wo[h][u]
            self.ao[u] = tanh(sum)
        return self.ao[:]

    def get_result(self, word_ids, url_ids):
        self.setup_network(word_ids, url_ids)
        return self.feed_forward()

    def back_propagate(self, targets, N=0.5):
        # calculate errors for output
        output_deltas = [0.0] * len(self.url_ids)
        for u in range(len(self.url_ids)):
            error = targets[u] - self.ao[u]
            output_deltas[u] = dtanh(self.ao[u]) * error

        # calculate errors for hidden layer
        hidden_deltas = [0.0] * len(self.hidden_ids)
        for h in range(len(self.hidden_ids)):
            error = 0.0
            for u in range(len(self.url_ids)):
                error = error + output_deltas[u] * self.wo[h][u]
            hidden_deltas[h] = dtanh(self.ah[h]) * error

        # update output weights
        for h in range(len(self.hidden_ids)):
            for u in range(len(self.url_ids)):
                change = output_deltas[u] * self.ah[h]
                self.wo[h][u] = self.wo[h][u] + N * change

        # update input weights
        for w in range(len(self.word_ids)):
            for h in range(len(self.hidden_ids)):
                change = hidden_deltas[h] * self.ai[w]
                self.wi[w][h] = self.wi[w][h] + N * change

    def trainquery(self, wordids, urlids, selectedurl):
        self.setup_network(wordids, urlids)
        self.feed_forward()
        targets = [0.0] * len(urlids)
        targets[urlids.index(selectedurl)] = 1.0
        error = self.back_propagate(targets)
        self.update_database()

    def update_database(self):
        for i in range(len(self.word_ids)):
            for j in range(len(self.hidden_ids)):
                self.set_strength(self.word_ids[i], self. hidden_ids[j], 0, self.wi[i][j])

        for j in range(len(self.hidden_ids)):
            for k in range(len(self.url_ids)):
                self.set_strength(self.hidden_ids[j], self.url_ids[k], 1, self.wo[j][k])
        self.con.commit()


def main():
    net = SearchNet('nn.db')
    # net.make_tables()
    wWorld, wRiver, wBank = 101, 102, 103
    uWorldBank, uRiver, uEarth = 201, 202, 203
    net.generate_hidden_node([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    result = net.get_result([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    print(result)
    net.trainquery([wWorld, wBank], [uWorldBank, uRiver, uEarth], uWorldBank)
    result = net.get_result([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    print(result)


if __name__ == "__main__":
    main()
