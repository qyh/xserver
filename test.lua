io.write('hello');
io.write('This is red->\27[1;31mredred\n')

print('This is red->\27[0;31mred\27[0m\n')
print('This is none->\27[0mnone\n')

io.flush()

