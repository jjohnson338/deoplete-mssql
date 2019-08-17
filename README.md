# deoplete-mssql

Deoplete-mssql offers asynchronous completion of SQL Server tables, views, columns, variables, and table aliases using `sqlcmd`.

![Example](https://user-images.githubusercontent.com/1017310/63215783-a4748480-c0f9-11e9-9c9e-79d1421c5268.png)

## Installation

To install `deoplete-mssql`, use your favourite plugin manager.

#### Using [vim-plug](https://github.com/junegunn/vim-plug) on neovim

```vim
Plug 'Shougo/deoplete.nvim', {'do': ':UpdateRemotePlugins'}
Plug 'jjohnson338/deoplete-mssql'
```

## Configuration
```vim
let g:deoplete#sources#mssql#server='localhost'
let g:deoplete#sources#mssql#user='SA'
let g:deoplete#sources#mssql#password='Seattle100'
let g:deoplete#sources#mssql#db='TestDB'
let g:deoplete#sources#mssql#case='upper' "Optional: Valid options are 'all', 'upper', 'lower'. Defaults to 'upper'
```
