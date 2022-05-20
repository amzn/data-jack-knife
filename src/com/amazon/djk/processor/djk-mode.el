;; djk-mode.el --- sample major mode for editing DJK.
;;
;; http://ergoemacs.org/emacs/elisp_syntax_coloring.html
;;
;; Copyright © 2015, by you

;; Author: your name ( your email )
;; Version: 2.0.13
;; Created: 26 Jun 2015
;; Keywords: languages
;; Homepage: http://ergoemacs.org/emacs/elisp_syntax_coloring.html

;; This file is not part of GNU Emacs.

;;; License:

;; You can redistribute this program and/or modify it under the terms of the GNU General Public License version 2.

;;; Commentary:

;; short description here

;; full doc on how to use here

;;; Code:

;; define several category of keywords
(setq djk-keywords '(%s)) ;; SUBSTITUTED
(setq djk-pipes '(%s)) ;; SUBSTITUTED
(setq djk-sources '(%s)) ;; SUBSTITUTED
(setq comment-start "#") ;; No workie!

;; generate regex string for each category of keywords
(setq djk-keywords-regexp (regexp-opt djk-keywords 'words))
(setq djk-pipes-regexp (regexp-opt djk-pipes 'words))
(setq djk-sources-regexp (regexp-opt djk-sources 'words))

;; create the list for font-lock.
;; each category of keyword is given a particular face
(setq djk-font-lock-keywords
      `(
        (,djk-keywords-regexp . font-lock-keyword-face)
        (,djk-pipes-regexp . font-lock-builtin-face)
        (,djk-sources-regexp . font-lock-function-name-face)
        ;; note: order above matters, because once colored, that part won't change.
        ;; in general, longer words first
        ))

(set-face-attribute 'font-lock-keyword-face nil :foreground "red" :weight 'bold) ;; keywords
(set-face-attribute 'font-lock-builtin-face nil :foreground "blue" :weight 'bold) ;; pipes
(set-face-attribute 'font-lock-function-name-face nil :foreground "green" :weight 'bold) ;; sources
(set-face-attribute 'font-lock-comment-face nil :foreground "black" :weight 'bold) ;; comments

(define-derived-mode djk-mode fundamental-mode
  "djk mode"
  "Major mode for editing DJK (Data Jack Knife Language)…"

  (setq comment-start "#") ;; No workie!
  ;; code for syntax highlighting
  (setq font-lock-defaults '((djk-font-lock-keywords))))

;; clear memory. no longer needed
(setq djk-keywords nil)
(setq djk-constants nil)
(setq djk-pipes nil)
(setq djk-sources nil)

;; clear memory. no longer needed
(setq djk-keywords-regexp nil)
(setq djk-constants-regexp nil)
(setq djk-pipes-regexp nil)
(setq djk-sources-regexp nil)

;; add the mode to the `features' list
(provide 'djk-mode)

;; Local Variables:
;; coding: utf-8
;; End:

;;; djk-mode.el ends here