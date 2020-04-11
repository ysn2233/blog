---
layout: post
title: "使用Attentioned Dual-Stage RNN模型预测股票走势（PyTorch）"
categories: ["AI"]
date: 2018-10-18T14:59:28+08:00
lastmod: 2018-10-18
draft: false
---
本项目复现了论文[A Dual-Stage Attention-Based Recurrent Neural Network for Time Series Prediction](https://arxiv.org/pdf/1704.02971.pdf)，利用PyTorch为框架实现了作者提出的基于attention机制的一个encoder-decoder模型，用于对股票价格的预测，全部代码请看https://github.com/ysn2233/attentioned-dual-stage-stock-prediction

####  数据集
数据集选用了NASDAQ 100 STOCK DATA中的AAPL.US和MSFT.US，Dataset的预处理类就不贴出来了详见github上的代码。大致是对Apple的股价数据集做切割去掉Microsoft上市之前的数据。


#### 模型
本文复现的模型属于Nonlinear Autoregressive with Exogenous Model(NARX)的一种，此类模型可以通过历史的时间序列值和一些外部输入的时间序列共同作用来预测在下一个时间节点的结果。在这个项目中，我使用苹果的股价作为目标时间序列，使用苹果的竞争对手微软的股价作为外部输入的驱动序列来预测下一天苹果的股价。

这个模型将拟合分为两个阶段，第一个Encoder阶段将结合外部输入的驱动序列和注意力机制（Attention mechanism）来抓取有用的信息组合成一段编码向量，其中Attention机制会作用于feature的维度。在第二个Decoder阶段将会结合之前的编码向量和目标时间序列的历史值来做最后的输出。在第二个阶段中的Attention机制会在时间维度上选取注意力点，为不同时间点的值来设定权重。因此最后的预测结果是基于时间和feature两个维度的attention机制。下面给出Encoder和Decoder的PyTorch定义，forward过程中每一步的维度都注释在语句之上。

##### Encoder
``` python
class AttnEncoder(nn.Module):

    def __init__(self, input_size, hidden_size, time_step):
        super(AttnEncoder, self).__init__()
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.T = time_step

        self.lstm = nn.LSTM(input_size=input_size, hidden_size=hidden_size, num_layers=1)
        self.attn1 = nn.Linear(in_features=2 * hidden_size, out_features=self.T)
        self.attn2 = nn.Linear(in_features=self.T, out_features=self.T)
        self.tanh = nn.Tanh()
        self.attn3 = nn.Linear(in_features=self.T, out_features=1)
        # self.attn = nn.Sequential(attn1, attn2, nn.Tanh(), attn3)

    def forward(self, driving_x):
        batch_size = driving_x.size(0)
        # batch_size * time_step * hidden_size
        code = self.init_variable(batch_size, self.T, self.hidden_size)
        # initialize hidden state
        h = self.init_variable(1, batch_size, self.hidden_size)
        # initialize cell state
        s = self.init_variable(1, batch_size, self.hidden_size)
        for t in range(self.T):
            # batch_size * input_size * (2 * hidden_size + time_step)
            x = torch.cat((self.embedding_hidden(h), self.embedding_hidden(s)), 2)
            z1 = self.attn1(x)
            z2 = self.attn2(driving_x.permute(0, 2, 1))
            x = z1 + z2
            # batch_size * input_size * 1
            z3 = self.attn3(self.tanh(x))
            if batch_size > 1:
                attn_w = F.softmax(z3.view(batch_size, self.input_size), dim=1)
            else:
                attn_w = self.init_variable(batch_size, self.input_size) + 1
            # batch_size * input_size
            weighted_x = torch.mul(attn_w, driving_x[:, t, :])
            _, states = self.lstm(weighted_x.unsqueeze(0), (h, s))
            h = states[0]
            s = states[1]

            # encoding result
            # batch_size * time_step * encoder_hidden_size
            code[:, t, :] = h

        return code

    @staticmethod
    def init_variable(*args):
        zero_tensor = torch.zeros(args)
        if torch.cuda.is_available():
            zero_tensor = zero_tensor.cuda()
        return Variable(zero_tensor)

    def embedding_hidden(self, x):
        return x.repeat(self.input_size, 1, 1).permute(1, 0, 2)
```
##### Decoder
``` python
class AttnDecoder(nn.Module):

    def __init__(self, code_hidden_size, hidden_size, time_step):
        super(AttnDecoder, self).__init__()
        self.code_hidden_size = code_hidden_size
        self.hidden_size = hidden_size
        self.T = time_step

        self.attn1 = nn.Linear(in_features=2 * hidden_size, out_features=code_hidden_size)
        self.attn2 = nn.Linear(in_features=code_hidden_size, out_features=code_hidden_size)
        self.tanh = nn.Tanh()
        self.attn3 = nn.Linear(in_features=code_hidden_size, out_features=1)
        self.lstm = nn.LSTM(input_size=1, hidden_size=self.hidden_size)
        self.tilde = nn.Linear(in_features=self.code_hidden_size + 1, out_features=1)
        self.fc1 = nn.Linear(in_features=code_hidden_size + hidden_size, out_features=hidden_size)
        self.fc2 = nn.Linear(in_features=hidden_size, out_features=1)

    def forward(self, h, y_seq):
        batch_size = h.size(0)
        d = self.init_variable(1, batch_size, self.hidden_size)
        s = self.init_variable(1, batch_size, self.hidden_size)
        ct = self.init_variable(batch_size, self.hidden_size)

        for t in range(self.T):
            # batch_size * time_step * (encoder_hidden_size + decoder_hidden_size)
            x = torch.cat((self.embedding_hidden(d), self.embedding_hidden(s)), 2)
            z1 = self.attn1(x)
            z2 = self.attn2(h)
            x = z1 + z2
            # batch_size * time_step * 1
            z3 = self.attn3(self.tanh(x))
            if batch_size > 1:
                beta_t = F.softmax(z3.view(batch_size, -1), dim=1)
            else:
                beta_t = self.init_variable(batch_size, self.code_hidden_size) + 1
            # batch_size * encoder_hidden_size
            ct = torch.bmm(beta_t.unsqueeze(1), h).squeeze(1)
            if t < self.T - 1:
                yc = torch.cat((y_seq[:, t].unsqueeze(1), ct), dim=1)
                y_tilde = self.tilde(yc)
                _, states = self.lstm(y_tilde.unsqueeze(0), (d, s))
                d = states[0]
                s = states[1]
        # batch_size * 1
        y_res = self.fc2(self.fc1(torch.cat((d.squeeze(0), ct), dim=1)))
        return y_res

    @staticmethod
    def init_variable(*args):
        zero_tensor = torch.zeros(args)
        if torch.cuda.is_available():
            zero_tensor = zero_tensor.cuda()
        return Variable(zero_tensor)

    def embedding_hidden(self, x):
        return x.repeat(self.T, 1, 1).permute(1, 0, 2)
```
#### 训练过程和可视化
``` python
class Trainer:

    def __init__(self, driving, target, time_step, split, lr):
        self.dataset = Dataset(driving, target, time_step, split)
        self.encoder = AttnEncoder(input_size=self.dataset.get_num_features(), hidden_size=config.ENCODER_HIDDEN_SIZE, time_step=time_step)
        self.decoder = AttnDecoder(code_hidden_size=config.ENCODER_HIDDEN_SIZE, hidden_size=config.DECODER_HIDDEN_SIZE, time_step=time_step)
        if torch.cuda.is_available():
            self.encoder = self.encoder.cuda()
            self.decoder = self.decoder.cuda()
        self.encoder_optim = optim.Adam(self.encoder.parameters(), lr)
        self.decoder_optim = optim.Adam(self.decoder.parameters(), lr)
        self.loss_func = nn.MSELoss()
        self.train_size, self.test_size = self.dataset.get_size()

    def train_minibatch(self, num_epochs, batch_size, interval):
        x_train, y_train, y_seq_train = self.dataset.get_train_set()
        for epoch in range(num_epochs):
            i = 0
            loss_sum = 0
            while i < self.train_size:
                self.encoder_optim.zero_grad()
                self.decoder_optim.zero_grad()
                batch_end = i + batch_size
                if batch_end >= self.train_size:
                    batch_end = self.train_size
                var_x = self.to_variable(x_train[i: batch_end])
                var_y = self.to_variable(y_train[i: batch_end])
                var_y_seq = self.to_variable(y_seq_train[i: batch_end])
                if var_x.dim() == 2:
                    var_x = var_x.unsqueeze(2)
                code = self.encoder(var_x)
                y_res = self.decoder(code, var_y_seq)
                loss = self.loss_func(y_res, var_y)
                loss.backward()
                self.encoder_optim.step()
                self.decoder_optim.step()
                # print('[%d], loss is %f' % (epoch, 10000 * loss.data[0]))
                loss_sum += loss.data[0]
                i = batch_end
            print('epoch [%d] finished, the average loss is %f' % (epoch, loss_sum))
            if (epoch + 1) % interval == 0 or epoch + 1 == num_epochs:
                torch.save(self.encoder.state_dict(), 'models/encoder' + str(epoch + 1) + '-norm' + '.model')
                torch.save(self.decoder.state_dict(), 'models/decoder' + str(epoch + 1) + '-norm' + '.model')

    def test(self, num_epochs, batch_size):
        x_train, y_train, y_seq_train = self.dataset.get_train_set()
        x_test, y_test, y_seq_test = self.dataset.get_test_set()
        y_pred_train = self.predict(x_train, y_train, y_seq_train, batch_size)
        y_pred_test = self.predict(x_test, y_test, y_seq_test, batch_size)
        plt.figure(figsize=(8,6), dpi=100)
        plt.plot(range(2000, self.train_size), y_train[2000:], label='train truth', color='black')
        plt.plot(range(self.train_size, self.train_size + self.test_size), y_test, label='ground truth', color='black')
        plt.plot(range(2000, self.train_size), y_pred_train[2000:], label='predicted train', color='red')
        plt.plot(range(self.train_size, self.train_size + self.test_size), y_pred_test, label='predicted test', color='blue')
        plt.xlabel('Days')
        plt.ylabel('Stock price of AAPL.US(USD)')
        plt.savefig('results/res-' + str(num_epochs) +'-' + str(batch_size) + '.png')

    def predict(self, x, y, y_seq, batch_size):
        y_pred = np.zeros(x.shape[0])
        i = 0
        while i < x.shape[0]:
            batch_end = i + batch_size
            if batch_end > x.shape[0]:
                batch_end = x.shape[0]
            var_x_input = self.to_variable(x[i: batch_end])
            var_y_input = self.to_variable(y_seq[i: batch_end])
            if var_x_input.dim() == 2:
                var_x_input = var_x_input.unsqueeze(2)
            code = self.encoder(var_x_input)
            y_res = self.decoder(code, var_y_input)
            for j in range(i, batch_end):
                y_pred[j] = y_res[j - i, -1]
            i = batch_end
        return y_pred

    def load_model(self, encoder_path, decoder_path):
        self.encoder.load_state_dict(torch.load(encoder_path, map_location=lambda storage, loc: storage))
        self.decoder.load_state_dict(torch.load(decoder_path, map_location=lambda storage, loc: storage))

    @staticmethod
    def to_variable(x):
        if torch.cuda.is_available():
            return Variable(torch.from_numpy(x).float()).cuda()
        else:
            return Variable(torch.from_numpy(x).float())
```

#### 结果
测试中对数据集作80%-20%的Cross validation，在128的batch-size下200到300个epochs的结果比较令人满意。但是比较高的拟合度也让人怀疑遇到了时间序列预测中经常碰到的“平移现象”-模型只预测结果为当时间点的前一天的值，这样测试曲线会与ground truth非常像但是却没有什么实际意义。利用时间序列分析预测股价是非常困难的，这个项目更多的是一种对模型的学习理解以及相关工程方面的练习。某些预测结果如下图所示，黑色为ground truth，红色为训练集的预测，蓝色为测试集的预测。
![128 Batchsize - 200 epochs](https://upload-images.jianshu.io/upload_images/13206628-96713838fd87b4e8.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
![128 Batchsize - 300 epochs](https://upload-images.jianshu.io/upload_images/13206628-0ec614befa2e0afa.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



