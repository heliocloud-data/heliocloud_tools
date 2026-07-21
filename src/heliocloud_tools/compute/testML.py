splits are # Train, Validation, Test

from hapi_nn import HAPINNTrainer, HAPINNTester, config
from tensorflow import keras
config.MODEL_ENGINE = 'TENSORFLOW'

trainer = HAPINNTrainer(splits, in_steps, out_steps, preprocess_func=None, lag=False)
data, meta = hapi(server, dataset, parameters, start, stop)
trainer.set_hapidatas([data], xyparameters=[['DST1800'], ['DST1800']]
trainer.prepare_data()

tester = HAPINNTester(in_steps, out_steps, preprocess_func=None)
data, meta = hapi(server, dataset, parameters, start, stop) 
tester.set_hapidatas([data], xyparameters=[['DST1800'], ['DST1800']])
tester.prepare_data()

x0 = keras.layers.Input(shape=(in_steps, 1))
x=keras.layers.Conv1D(compression//2,5,strides=2,padding='same',activation='swish')(x0)
for i in range(5): x=keras.layers.Conv1D(compression,5,strides=2,padding='same',activation='swish')(x)
x=keras.layers.Dropout(dropout)(x)
x=keras.layers.Conv1DTranspose(compression,3,strides=3)(x)
for i in range 5: x=keras.layers.Conv1DTranspose(compression,5,strides=2,padding='same')(x)
x=keras.layers.Conv1DTranspose(compression//2,5,strides=2,padding='same')(x)
x=keras.layers.Conv1DTranspose(1,1,strides=1,padding='same')(x)

model = keras.models.Model(inputs=x0, outputs=x)
optimizer = keras.optimizers.Adam(learning_rate=0.0005)
model.compile(loss='mse', optimizer=optimizer, metrics=['mae'])

trainer.train(model, epochs, batch_size=batch_size)

predictions = tester.test(model)
tester.plot(predictions, 9, 'DST1800')
