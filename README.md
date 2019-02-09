# deoplete-mssql

Deoplete-mssql offers asynchronous completion of SQL Server tables, views, columns, and table aliases using `sqlcmd`.

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
```
